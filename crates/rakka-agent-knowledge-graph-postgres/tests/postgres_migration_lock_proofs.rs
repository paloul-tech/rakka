//! A migrator that cannot take the migration lock stops waiting and names
//! the session that holds it (issue #143).
//!
//! `migrate` holds a session advisory lock while it works. A call that is
//! dropped or timed out part-way leaves the lock on its connection, and
//! every other migrator used to wait for it without a bound and without an
//! error. A waiter now gives up after the store's lock wait, and its refusal
//! says which connection to close.
//!
//! The lock is one per database, whatever schema a session resolves its
//! tables in. So the proofs here are **one test in a test binary of their
//! own**: no other test's `migrate` contends for the lock while this one
//! holds it, and nothing but this test's holder can be the session a refusal
//! names.
//!
//! Gated on `RAKKA_POSTGRES_TEST_DSN`; passes silently without it.

use std::sync::Arc;
use std::time::Duration;

use rakka_agent_knowledge_graph_postgres::{
    PostgresKnowledgeGraphStore, DEFAULT_MIGRATION_LOCK_WAIT, MIGRATION_LOCK_ID,
};
use tokio_postgres::{Client, NoTls};

/// Longer than any refusal here should take, and far shorter than a wait
/// with no bound.
const MARGIN: Duration = Duration::from_secs(5);

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .expect("the PostgreSQL test database should connect");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres test connection error: {error}");
        }
    });
    client
}

#[tokio::test]
async fn postgres_a_migrator_stops_waiting_and_names_the_holder_when_dsn_is_set() {
    let Ok(dsn) = std::env::var("RAKKA_POSTGRES_TEST_DSN") else {
        return;
    };

    // The default, and the builder that replaces it.
    assert_eq!(DEFAULT_MIGRATION_LOCK_WAIT, Duration::from_secs(300));
    let default = PostgresKnowledgeGraphStore::new(connect(&dsn).await);
    assert_eq!(default.migration_lock_wait(), DEFAULT_MIGRATION_LOCK_WAIT);
    default.migrate().await.expect("the schema applies");

    // A session takes the lock and never releases it, as a cancelled
    // `migrate` leaves it. It labels itself, so the refusal can be shown not
    // to repeat the label.
    let holder = connect(&dsn).await;
    holder
        .batch_execute("SET application_name = 'holder-application-name'")
        .await
        .expect("the holder labels itself");
    holder
        .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
        .await
        .expect("the holder takes the lock");
    let holder_pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("the holder knows its backend")
        .get(0);

    // A waiter with a short wait gives up inside the margin and names the
    // holder.
    let waiter = PostgresKnowledgeGraphStore::new(connect(&dsn).await)
        .with_migration_lock_wait(Duration::from_millis(300));
    assert_eq!(waiter.migration_lock_wait(), Duration::from_millis(300));
    let refused = tokio::time::timeout(MARGIN, waiter.migrate())
        .await
        .expect("the waiter stops inside the margin")
        .expect_err("the lock is held");
    assert_eq!(refused.code(), "claim-backend-failed");
    let message = refused.to_string();
    assert!(
        message.contains("migration lock not acquired within 300ms"),
        "{message}"
    );
    assert!(
        message.contains(&format!("held by backend pid {holder_pid}")),
        "{message}"
    );
    assert!(message.contains("connected for "), "{message}");
    assert!(message.contains("state idle"), "{message}");
    for forbidden in [
        "holder-application-name",
        "pg_advisory_lock",
        "postgres://",
        dsn.as_str(),
    ] {
        assert!(
            !message.contains(forbidden),
            "the refusal carries {forbidden}: {message}"
        );
    }

    // A wait of zero is one try.
    let once = PostgresKnowledgeGraphStore::new(connect(&dsn).await)
        .with_migration_lock_wait(Duration::ZERO);
    tokio::time::timeout(MARGIN, once.migrate())
        .await
        .expect("one try ends at once")
        .expect_err("the lock is held");

    // The holder's connection closes, which is what the refusal tells an
    // operator to do. A store with the default wait covers the moment the
    // server takes to notice; after it, the two that were refused migrate.
    drop(holder);
    default
        .migrate()
        .await
        .expect("the default wait covers the close");
    waiter.migrate().await.expect("the lock is free");
    once.migrate().await.expect("one try takes a free lock");

    // A session lock is re-entrant: a retry on the connection that already
    // holds the lock still succeeds, as `migrate` documents.
    let shared = Arc::new(connect(&dsn).await);
    shared
        .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
        .await
        .expect("the connection takes the lock");
    PostgresKnowledgeGraphStore::from_shared_client(shared.clone())
        .with_migration_lock_wait(Duration::ZERO)
        .migrate()
        .await
        .expect("a session lock is re-entrant");
}

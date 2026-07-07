//! Gated PostgreSQL projection-store tests.
//!
//! Skipped unless `RAKKA_POSTGRES_TEST_DSN` points at a reachable database:
//!
//! ```sh
//! RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p rakka-a2a --features postgres --test postgres_projection
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use a2a::{ListTasksRequest, TaskState};
use rakka_a2a::postgres::{
    connect_shared_postgres_client, PostgresA2ATaskEventWatcher, PostgresA2ATaskProjectionStore,
};
use rakka_a2a::projection::{
    A2ATaskEventRetention, A2ATaskEventSignalOutcome, A2ATaskEventWatcher, A2ATaskProjectionStore,
};
use rakka_a2a::task::{A2ATaskEvent, A2ATaskEventPayload, A2ATaskProjection, TaskProjectionError};
use rakka_agent_workflow::AgentTimestampMillis;

fn test_dsn() -> Option<String> {
    std::env::var("RAKKA_POSTGRES_TEST_DSN").ok()
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{prefix}-{nanos}-{}", std::process::id())
}

async fn store(dsn: &str) -> PostgresA2ATaskProjectionStore {
    let client = connect_shared_postgres_client(dsn).await.expect("connect");
    let store = PostgresA2ATaskProjectionStore::from_shared_client(client);
    store.migrate().await.expect("migrate");
    store
}

fn accepted(tenant: &str, task_id: &str, revision: u64) -> A2ATaskProjection {
    A2ATaskProjection::accepted(
        task_id,
        "ctx",
        tenant,
        "workflow-pg-test",
        AgentTimestampMillis::new(10),
        Vec::new(),
        revision,
    )
}

async fn bootstrap(store: &PostgresA2ATaskProjectionStore, tenant: &str, task_id: &str) {
    store
        .append_event_payload(
            tenant,
            task_id,
            "ctx",
            AgentTimestampMillis::new(10),
            A2ATaskEventPayload::Snapshot(accepted(tenant, task_id, 0)),
        )
        .await
        .expect("bootstrap snapshot");
}

#[tokio::test]
async fn migrations_apply_cleanly_and_are_idempotent() {
    let Some(dsn) = test_dsn() else { return };
    let first = store(&dsn).await;
    // Second apply (fresh client) must be a no-op, and concurrent applies
    // serialize on the advisory lock instead of failing.
    let second = store(&dsn).await;
    let (left, right) = tokio::join!(first.migrate(), second.migrate());
    left.expect("concurrent migrate");
    right.expect("concurrent migrate");
}

#[tokio::test]
async fn projection_and_events_survive_restart_and_serve_any_node() {
    let Some(dsn) = test_dsn() else { return };
    let tenant = unique("tenant-restart");
    let task_id = unique("task-restart");

    let writer = store(&dsn).await;
    bootstrap(&writer, &tenant, &task_id).await;
    writer
        .append_event_payload(
            &tenant,
            &task_id,
            "ctx",
            AgentTimestampMillis::new(11),
            A2ATaskEventPayload::StatusUpdate {
                state: TaskState::Working,
            },
        )
        .await
        .expect("status event");

    // A second store over a second connection models another node (or the
    // same node after restart) reading shared durable state.
    let reader = store(&dsn).await;
    let projection = reader
        .projection(Some(&tenant), &task_id)
        .await
        .expect("projection");
    assert_eq!(projection.status, TaskState::Working);
    assert_eq!(projection.projection_revision, 2);

    let task = reader
        .get(Some(&tenant), &task_id, None)
        .await
        .expect("get");
    assert_eq!(task.id, task_id);

    let events = reader
        .replay_events(&tenant, &task_id, None)
        .await
        .expect("replay");
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn tenant_scoped_queries_do_not_leak_foreign_tenants() {
    let Some(dsn) = test_dsn() else { return };
    let tenant_a = unique("tenant-a");
    let tenant_b = unique("tenant-b");
    // Same task id under both tenants: scoping must come from the key, not
    // from id uniqueness.
    let task_id = unique("task-scoped");

    let store = store(&dsn).await;
    bootstrap(&store, &tenant_a, &task_id).await;
    bootstrap(&store, &tenant_b, &task_id).await;
    store
        .append_event_payload(
            &tenant_b,
            &task_id,
            "ctx",
            AgentTimestampMillis::new(11),
            A2ATaskEventPayload::Terminal {
                state: TaskState::Completed,
            },
        )
        .await
        .expect("terminal for tenant-b");

    let a = store
        .projection(Some(&tenant_a), &task_id)
        .await
        .expect("tenant-a projection");
    assert_eq!(a.tenant, tenant_a);
    assert_eq!(a.status, TaskState::Submitted);

    let b = store
        .projection(Some(&tenant_b), &task_id)
        .await
        .expect("tenant-b projection");
    assert_eq!(b.status, TaskState::Completed);

    // Unscoped reads are refused outright in this store (DN-3).
    let unscoped = store
        .projection(None, &task_id)
        .await
        .expect_err("unscoped");
    assert_eq!(unscoped.code(), "tenant-required");
    let unscoped_list = store
        .list(&ListTasksRequest {
            tenant: None,
            context_id: None,
            status: None,
            page_size: None,
            page_token: None,
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: None,
        })
        .await
        .expect_err("unscoped list");
    assert_eq!(unscoped_list.code(), "tenant-required");

    // A tenant-scoped list never returns a foreign-tenant row.
    let listed = store
        .list(&ListTasksRequest {
            tenant: Some(tenant_a.clone()),
            context_id: None,
            status: None,
            page_size: None,
            page_token: None,
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: None,
        })
        .await
        .expect("tenant-a list");
    assert_eq!(listed.tasks.len(), 1);
    assert_eq!(listed.total_size, 1);

    // Foreign-tenant reads are indistinguishable from missing tasks.
    let foreign = store
        .projection(Some(&unique("tenant-c")), &task_id)
        .await
        .expect_err("foreign tenant");
    assert_eq!(foreign.code(), "task-not-found");
}

#[tokio::test]
async fn list_paginates_deterministically() {
    let Some(dsn) = test_dsn() else { return };
    let tenant = unique("tenant-page");
    let store = store(&dsn).await;
    for index in 0..3 {
        let task_id = format!("{}-{index}", unique("task-page"));
        bootstrap(&store, &tenant, &task_id).await;
    }

    let request = ListTasksRequest {
        tenant: Some(tenant.clone()),
        context_id: Some("ctx".to_string()),
        status: Some(TaskState::Submitted),
        page_size: Some(2),
        page_token: None,
        history_length: None,
        status_timestamp_after: None,
        include_artifacts: None,
    };
    let page1 = store.list(&request).await.expect("page1");
    assert_eq!(page1.tasks.len(), 2);
    assert_eq!(page1.total_size, 3);
    assert_eq!(page1.next_page_token, "2");

    let page2 = store
        .list(&ListTasksRequest {
            page_token: Some(page1.next_page_token),
            ..request
        })
        .await
        .expect("page2");
    assert_eq!(page2.tasks.len(), 1);
    assert!(page2.next_page_token.is_empty());

    let all_ids = page1
        .tasks
        .iter()
        .chain(page2.tasks.iter())
        .map(|task| task.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(all_ids.len(), 3, "pages must not overlap or skip");
}

#[tokio::test]
async fn replay_cursor_resumes_across_nodes_without_gap_or_duplicate() {
    let Some(dsn) = test_dsn() else { return };
    let tenant = unique("tenant-cursor");
    let task_id = unique("task-cursor");

    let writer = store(&dsn).await;
    bootstrap(&writer, &tenant, &task_id).await;
    for index in 0..4_u64 {
        writer
            .append_event_payload(
                &tenant,
                &task_id,
                "ctx",
                AgentTimestampMillis::new(11 + index),
                A2ATaskEventPayload::StatusUpdate {
                    state: TaskState::Working,
                },
            )
            .await
            .expect("status event");
    }

    let reader = store(&dsn).await;
    let cursor = format!("{task_id}:2");
    let resumed = reader
        .replay_events(&tenant, &task_id, Some(&cursor))
        .await
        .expect("resume");
    assert_eq!(
        resumed
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4, 5],
        "resume must have no gap and no duplicate at the seam"
    );

    let future_cursor = format!("{task_id}:99");
    let error = reader
        .replay_events(&tenant, &task_id, Some(&future_cursor))
        .await
        .expect_err("future cursor");
    assert_eq!(error.code(), "invalid-replay-cursor");

    let foreign_cursor = "other-task:2";
    let error = reader
        .replay_events(&tenant, &task_id, Some(foreign_cursor))
        .await
        .expect_err("foreign cursor");
    assert_eq!(error.code(), "invalid-replay-cursor");
}

#[tokio::test]
async fn retention_compacts_but_preserves_snapshot_and_reports_expired_window() {
    let Some(dsn) = test_dsn() else { return };
    let tenant = unique("tenant-retain");
    let task_id = unique("task-retain");

    let client = connect_shared_postgres_client(&dsn).await.expect("connect");
    let store = PostgresA2ATaskProjectionStore::from_shared_client(client)
        .with_retention(A2ATaskEventRetention::new(4));
    store.migrate().await.expect("migrate");

    bootstrap(&store, &tenant, &task_id).await;
    for index in 0..8_u64 {
        store
            .append_event_payload(
                &tenant,
                &task_id,
                "ctx",
                AgentTimestampMillis::new(11 + index),
                A2ATaskEventPayload::StatusUpdate {
                    state: TaskState::Working,
                },
            )
            .await
            .expect("status event");
    }
    let terminal = store
        .append_event_payload(
            &tenant,
            &task_id,
            "ctx",
            AgentTimestampMillis::new(30),
            A2ATaskEventPayload::Terminal {
                state: TaskState::Completed,
            },
        )
        .await
        .expect("terminal event");
    assert_eq!(terminal.sequence, 10);

    // The terminal snapshot survives compaction through the projection row.
    let projection = store
        .projection(Some(&tenant), &task_id)
        .await
        .expect("projection");
    assert_eq!(projection.status, TaskState::Completed);
    assert_eq!(projection.projection_revision, 10);

    // Replay from before the retained window must resync, not silently skip.
    let expired = store
        .replay_events(&tenant, &task_id, None)
        .await
        .expect_err("expired window");
    assert!(matches!(
        expired,
        TaskProjectionError::ReplayWindowExpired { .. }
    ));

    // The newest snapshot event (the bootstrap) is preserved by compaction.
    let events_with_snapshot = store
        .replay_events(&tenant, &task_id, Some(&format!("{task_id}:6")))
        .await
        .expect("tail replay");
    assert_eq!(
        events_with_snapshot
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![7, 8, 9, 10]
    );
}

#[tokio::test]
async fn fixed_sequence_append_rejects_out_of_order_events() {
    let Some(dsn) = test_dsn() else { return };
    let tenant = unique("tenant-order");
    let task_id = unique("task-order");
    let store = store(&dsn).await;
    bootstrap(&store, &tenant, &task_id).await;

    let stale = A2ATaskEvent::new(
        tenant.clone(),
        task_id.clone(),
        "ctx",
        5,
        AgentTimestampMillis::new(20),
        A2ATaskEventPayload::StatusUpdate {
            state: TaskState::Working,
        },
    );
    let error = store.append_event(stale).await.expect_err("out of order");
    assert_eq!(error.code(), "event-order");
}

#[tokio::test]
async fn polling_watcher_signals_new_durable_events() {
    let Some(dsn) = test_dsn() else { return };
    let tenant = unique("tenant-watch");
    let task_id = unique("task-watch");

    let client = connect_shared_postgres_client(&dsn).await.expect("connect");
    let store = PostgresA2ATaskProjectionStore::from_shared_client(client.clone());
    store.migrate().await.expect("migrate");
    bootstrap(&store, &tenant, &task_id).await;

    let watcher = PostgresA2ATaskEventWatcher::from_shared_client(client)
        .with_poll_interval(Duration::from_millis(50));
    let mut signal = watcher.watch(&tenant, &task_id).await.expect("watch");

    store
        .append_event_payload(
            &tenant,
            &task_id,
            "ctx",
            AgentTimestampMillis::new(11),
            A2ATaskEventPayload::StatusUpdate {
                state: TaskState::Working,
            },
        )
        .await
        .expect("status event");

    let outcome = tokio::time::timeout(Duration::from_secs(5), signal.changed())
        .await
        .expect("watcher must signal within deadline");
    assert_eq!(
        outcome,
        A2ATaskEventSignalOutcome::Notified {
            high_watermark_hint: 2
        }
    );
}

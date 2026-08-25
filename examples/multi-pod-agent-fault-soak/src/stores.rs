//! The shared durable substrate every pod reads and writes.
//!
//! An in-process test can share an [`rakka_persistence::InMemoryDurableStateStore`]
//! between "pods" because they are the same process. A pod that really dies
//! takes its process memory with it, so the store has to be somewhere else —
//! which is the whole point of [specification 15](../../../docs/plans/rakka-agent/spec.md):
//! durable state MUST be sufficient to recover an agent, task, or run on a
//! different pod *without node-local memory*.
//!
//! Here that somewhere else is a shared directory. Each committed revision is
//! one file, and the commit is a `hard_link` from a fully written temporary
//! file to the revision's name: the link is atomic and exclusive on POSIX
//! filesystems, so two pods racing the same compare-and-set can never both
//! win — the loser sees the existing revision and reports a revision conflict.
//! That is the same fence a PostgreSQL `WHERE revision = $expected` gives, and
//! it is what makes stale-owner rejection real here rather than simulated.
//!
//! **This is a harness, not a recommendation.** A shared directory on one host
//! stands in for the shared durable backend; production is PostgreSQL through
//! `rakka_persistence_postgres::PostgresDurableStateStore`, which is already
//! generic over the state type. Specification 15 forbids pod-local state as the
//! production source of truth, and a shared volume is not what it means by
//! "pod-local" — but neither is it a database.

use std::fmt::Write as _;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_persistence::{
    DurableError, DurableResult, DurableState, DurableStateStore, PersistenceId, Revision,
    StateRecord, StoreFuture,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The file a pod writes immediately before it aborts.
///
/// The sweep's termination condition, and its proof of work. A window whose
/// armed write the flow never reached leaves no marker, which is how the driver
/// knows a row is spent rather than guessing its length from a different run —
/// and because the marker is written only on the abort path, a counted window
/// is one that demonstrably fired.
pub const CRASHED: &str = "crashed";

/// Monotonic per-process suffix keeping temp file names collision-free.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn hex_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn hex_decode(value: &str) -> Option<String> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(chunk).ok()?;
        bytes.push(u8::from_str_radix(pair, 16).ok()?);
    }
    String::from_utf8(bytes).ok()
}

fn store_error(error: impl ToString) -> DurableError {
    DurableError::store("multi-pod-file", error.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStateRecord<S> {
    persistence_id: PersistenceId,
    revision: u64,
    state: S,
}

/// A durable state store whose records live in a shared directory.
#[derive(Debug)]
pub struct SharedFileStore<S>
where
    S: DurableState,
{
    root: Arc<PathBuf>,
    _state: PhantomData<fn() -> S>,
}

impl<S> SharedFileStore<S>
where
    S: DurableState,
{
    /// A store rooted at `root`, which every pod is given the same path to.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
            _state: PhantomData,
        }
    }

    fn revision_path(&self, persistence_id: &PersistenceId, revision: Revision) -> PathBuf {
        self.root.join(format!(
            "{}.r{:020}.json",
            hex_encode(persistence_id.as_str()),
            revision.get()
        ))
    }

    fn parse_record_file_name(file_name: &str) -> Option<(String, Revision)> {
        let stem = file_name.strip_suffix(".json")?;
        let (hex, digits) = stem.rsplit_once(".r")?;
        let revision = digits.parse::<u64>().ok()?;
        Some((hex_decode(hex)?, Revision::new(revision)))
    }

    fn record_files(&self) -> DurableResult<Vec<(String, Revision, PathBuf)>> {
        let entries = match std::fs::read_dir(self.root.as_ref()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(store_error(error)),
        };
        let mut records = Vec::new();
        for entry in entries {
            let path = entry.map_err(store_error)?.path();
            let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            let Some((id, revision)) = Self::parse_record_file_name(file_name) else {
                continue;
            };
            records.push((id, revision, path));
        }
        Ok(records)
    }

    fn current_revision_file(
        &self,
        persistence_id: &PersistenceId,
    ) -> DurableResult<Option<(Revision, PathBuf)>> {
        let mut newest: Option<(Revision, PathBuf)> = None;
        for (id, revision, path) in self.record_files()? {
            if id != persistence_id.as_str() {
                continue;
            }
            if newest
                .as_ref()
                .is_none_or(|(current, _)| revision > *current)
            {
                newest = Some((revision, path));
            }
        }
        Ok(newest)
    }

    fn current_revision(&self, persistence_id: &PersistenceId) -> DurableResult<Revision> {
        Ok(self
            .current_revision_file(persistence_id)?
            .map_or(Revision::INITIAL, |(revision, _)| revision))
    }
}

impl<S> SharedFileStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    fn load_record(&self, persistence_id: &PersistenceId) -> DurableResult<Option<StateRecord<S>>> {
        let Some((_, path)) = self.current_revision_file(persistence_id)? else {
            return Ok(None);
        };
        let bytes = std::fs::read(&path).map_err(store_error)?;
        let stored: StoredStateRecord<S> = serde_json::from_slice(&bytes)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        if stored.persistence_id != *persistence_id {
            return Err(DurableError::codec(format!(
                "record path for {persistence_id} contained {}",
                stored.persistence_id
            )));
        }
        Ok(Some(StateRecord::new(
            stored.state,
            Revision::new(stored.revision),
        )))
    }

    /// Commits `record` as the exclusive winner of its revision.
    ///
    /// The `hard_link` is the claim: a pod that also passed the revision check
    /// loses the link and surfaces a revision conflict rather than a lost
    /// update. Two live pods for one entity is not a hypothetical here — it is
    /// what a shard handoff looks like from the store's side.
    fn commit_record(
        &self,
        persistence_id: &PersistenceId,
        record: &StateRecord<S>,
    ) -> DurableResult<()> {
        std::fs::create_dir_all(self.root.as_ref()).map_err(store_error)?;
        let path = self.revision_path(persistence_id, record.revision);
        let temp = path.with_extension(format!(
            "json.tmp-{}-{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let stored = StoredStateRecord {
            persistence_id: persistence_id.clone(),
            revision: record.revision.get(),
            state: record.state.clone(),
        };
        let bytes =
            serde_json::to_vec(&stored).map_err(|error| DurableError::codec(error.to_string()))?;
        let write_result = (|| {
            use std::io::Write as _;
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()
        })();
        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&temp);
            return Err(store_error(error));
        }
        let linked = std::fs::hard_link(&temp, &path);
        let _ = std::fs::remove_file(&temp);
        match linked {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let actual = self.current_revision(persistence_id)?;
                Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    Revision::new(record.revision.get().saturating_sub(1)),
                    actual,
                ))
            }
            Err(error) => Err(store_error(error)),
        }
    }
}

impl<S> Clone for SharedFileStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            _state: PhantomData,
        }
    }
}

/// Unlinks one revision file, tolerating one a concurrent pass already removed.
fn remove_record_file(path: &std::path::Path) -> DurableResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(store_error(error)),
    }
}

impl<S> DurableStateStore<S> for SharedFileStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    fn backend_name(&self) -> &'static str {
        "multi-pod-file"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        Box::pin(async move { self.load_record(persistence_id) })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        Box::pin(async move {
            let actual = self.current_revision(persistence_id)?;
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }
            let record = StateRecord::new(state, expected_revision.next());
            self.commit_record(persistence_id, &record)?;
            Ok(record)
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        Box::pin(async move {
            let actual = self.current_revision(persistence_id)?;
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }
            let Some(current) = self.load_record(persistence_id)? else {
                // `expected_revision` matched `Revision::INITIAL`: nothing was
                // ever committed for this id, so there is nothing to fence and
                // nothing to unlink.
                return Ok(Revision::INITIAL);
            };

            // A delete mutates the record exactly as a commit does, so it takes
            // the same claim: a `hard_link` on the next revision, won by exactly
            // one pod. Without it the unlink loop below is a read-then-act — a
            // pod that commits `expected + 1` between the revision check and the
            // loop has its committed revision unlinked, which is the lost update
            // the link exists to prevent and which a `DELETE ... WHERE revision
            // = $expected` would have refused.
            let claim = StateRecord::new(current.state, expected_revision.next());
            self.commit_record(persistence_id, &claim)?;

            // Holding the claim, no other pod can commit for this id, so every
            // file left is one this delete is entitled to remove. The claim goes
            // last: a pod lost mid-delete leaves the record readable at the
            // claimed revision — a delete that did not happen — instead of a
            // hole an older revision resurrects through.
            let mut claim_path = None;
            for (id, revision, path) in self.record_files()? {
                if id != persistence_id.as_str() {
                    continue;
                }
                if revision == claim.revision {
                    claim_path = Some(path);
                    continue;
                }
                remove_record_file(&path)?;
            }
            if let Some(path) = claim_path {
                remove_record_file(&path)?;
            }
            // What both reference stores return, and what this store's own
            // `current_revision` reports now that every file is unlinked:
            // `InMemoryDurableStateStore::delete` removes the record and
            // answers `Revision::INITIAL`, and `PostgresDurableStateStore`
            // does the same on a row it deleted. Answering `expected_revision`
            // instead hands the caller a revision no later
            // `compare_and_set` can ever match.
            Ok(Revision::INITIAL)
        })
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        Box::pin(async move {
            let mut ids = self
                .record_files()?
                .into_iter()
                .map(|(id, _, _)| PersistenceId::new(id))
                .collect::<Vec<_>>();
            ids.sort();
            ids.dedup();
            Ok(ids)
        })
    }
}

/// Where a pod dies relative to one durable write.
///
/// The in-process [`rakka_agent::testkit::CrashPoint`] models the same two
/// windows by *returning an error*, which is the right model when the owner is
/// a struct the test still holds. A pod does not get to return: it stops, its
/// leases go stale, and everything it had not yet written is gone. So the
/// decorator here aborts the process instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodCrash {
    /// The pod dies before the write reaches the shared store.
    BeforeWrite,
    /// The pod dies after the write commits but before it can act on what it
    /// just decided. This is the window that matters: the record says one
    /// thing and nobody has been told.
    AfterWrite,
}

impl PodCrash {
    /// Parses the driver's `--crash-at` argument.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "before-write" => Some(Self::BeforeWrite),
            "after-write" => Some(Self::AfterWrite),
            _ => None,
        }
    }

    /// Stable kebab-case label, as the driver passes it.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::BeforeWrite => "before-write",
            Self::AfterWrite => "after-write",
        }
    }
}

/// A store that kills its own pod at the `nth` write.
///
/// `nth` is 1-based, and counts `compare_and_set` and `delete` alike. Once the
/// armed write is reached the process is gone, so — unlike the in-process
/// crash store — there is no "and then it survived": the recovery is performed
/// by a *different* process reading the same directory.
#[derive(Debug)]
pub struct PodCrashStore<S>
where
    S: DurableState,
{
    inner: SharedFileStore<S>,
    writes: Arc<AtomicUsize>,
    armed: Option<(usize, PodCrash)>,
    marker: Option<PathBuf>,
}

impl<S> PodCrashStore<S>
where
    S: DurableState,
{
    /// A store that never kills its pod.
    #[must_use]
    pub fn new(inner: SharedFileStore<S>) -> Self {
        Self {
            inner,
            writes: Arc::new(AtomicUsize::new(0)),
            armed: None,
            marker: None,
        }
    }

    /// Arms the pod to die at the `nth` write from now.
    ///
    /// `marker` is where the dying pod records the window it died in, so the
    /// driver can tell a window that fired from one whose armed write this
    /// pod's flow never reached.
    #[must_use]
    pub fn armed_at(mut self, nth: usize, crash: PodCrash, marker: PathBuf) -> Self {
        debug_assert!(nth >= 1, "write ordinals are 1-based");
        self.armed = Some((nth, crash));
        self.marker = Some(marker);
        self
    }

    /// How many writes this pod has attempted.
    #[must_use]
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    /// Reserves this write's ordinal.
    ///
    /// The counter runs whether or not a crash is armed, so every pod can
    /// report its writes in its exit line. Those counts are what a reader sizes
    /// the flow by; the sweep no longer takes its row lengths from them, which
    /// is what [`CRASHED`] is for.
    ///
    /// One store operation reserves one ordinal and carries it into both of
    /// its crash windows. Reading the counter back for the after-write window
    /// would name whatever ordinal the *last* write to start reserved: the
    /// sharded entity actors and the drive loop write through clones of one
    /// store, so a write that overlaps this one moves the counter between the
    /// two windows — firing the abort inside the wrong write, or losing it.
    fn reserve_write(&self) -> usize {
        self.writes.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Dies if `ordinal` is the armed write and this is its armed window.
    fn maybe_die(&self, ordinal: usize, before: bool) {
        let Some((nth, crash)) = self.armed else {
            return;
        };
        let dies = ordinal == nth
            && match crash {
                PodCrash::BeforeWrite => before,
                PodCrash::AfterWrite => !before,
            };
        if dies {
            // Recorded before the abort, not after: nothing runs after it. A
            // marker that fails to land leaves a pod that never reported its
            // writes and never announced a crash, which the driver refuses to
            // read as either a spent sweep row or a skip.
            if let Some(marker) = &self.marker {
                if let Err(error) = std::fs::write(marker, format!("{nth} {}", crash.as_label())) {
                    eprintln!("could not record the crash marker: {error}");
                }
            }
            // Not a panic: a panic unwinds, runs destructors, and lets the
            // harness observe an orderly failure. A pod loss is none of those.
            if std::env::var("RAKKA_MULTI_POD_VERBOSE").is_ok() {
                eprintln!("pod dying at write {nth} ({})", crash.as_label());
            }
            std::process::abort();
        }
    }
}

impl<S> Clone for PodCrashStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            writes: self.writes.clone(),
            armed: self.armed,
            marker: self.marker.clone(),
        }
    }
}

impl<S> DurableStateStore<S> for PodCrashStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        Box::pin(async move {
            let ordinal = self.reserve_write();
            self.maybe_die(ordinal, true);
            let record = self
                .inner
                .compare_and_set(persistence_id, expected_revision, state)
                .await;
            // Deliberately not `?` before the second window: the ordinal is
            // spent either way, and a pod dies where it dies. A revision
            // conflict is a write that reached the store and was refused, not
            // a write that never happened — short-circuiting past the window
            // would leave that ordinal in the sweep plan but silent.
            self.maybe_die(ordinal, false);
            record
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        Box::pin(async move {
            let ordinal = self.reserve_write();
            self.maybe_die(ordinal, true);
            let revision = self.inner.delete(persistence_id, expected_revision).await;
            self.maybe_die(ordinal, false);
            revision
        })
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        self.inner.persistence_ids()
    }
}

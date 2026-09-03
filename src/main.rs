use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::Mutex;

const CORRELATION_WINDOW: Duration = Duration::from_secs(60);
const DEDUP_WINDOW: Duration = Duration::from_secs(10);
const NUM_SHARDS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
enum EventKind {
    FailedLogin,
    LoginSuccess,
}

#[derive(Debug, Clone)]
struct LogEvent {
    event_id: u64,
    username: String,
    kind: EventKind,
    timestamp: Instant,
}

#[derive(Debug)]
struct Alert {
    username: String,
    failed_count: usize,
}

/// State protected by one shard's mutex.
struct ShardState {
    /// Recently seen event IDs -> time they were received.
    seen_events: HashMap<u64, Instant>,

    /// username -> timestamp-ordered failed login events.
    ///
    /// usize allows multiple failures at exactly the same timestamp.
    failures: HashMap<String, BTreeMap<Instant, usize>>,
}

struct Shard {
    state: Mutex<ShardState>,
}

struct CorrelationEngine {
    shards: Vec<Arc<Shard>>,
}

impl CorrelationEngine {
    fn new() -> Self {
        let shards = (0..NUM_SHARDS)
            .map(|_| {
                Arc::new(Shard {
                    state: Mutex::new(ShardState {
                        seen_events: HashMap::new(),
                        failures: HashMap::new(),
                    }),
                })
            })
            .collect();

        Self { shards }
    }

    /// Map a username to a deterministic shard.
    ///
    /// Different usernames can therefore use different mutexes,
    /// avoiding one global lock for the entire engine.
    fn shard_index(&self, username: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        username.hash(&mut hasher);

        (hasher.finish() as usize) % self.shards.len()
    }

    /// Ingest one event.
    ///
    /// Returns Some(Alert) when this event completes:
    ///
    ///     5+ failed logins
    ///              +
    ///     login success
    ///              +
    ///     same username
    ///              +
    ///     within 60 seconds
    async fn ingest(&self, event: LogEvent) -> Option<Alert> {
        let shard_index = self.shard_index(&event.username);
        let shard = &self.shards[shard_index];

        let mut state = shard.state.lock().await;

        // ---------------------------------------------------------
        // 1. Deduplication
        // ---------------------------------------------------------

        /*
         * Deduplication is based on arrival time.
         *
         * The requirement says duplicate event IDs arriving within
         * a short window should be ignored.
         */
        let now = Instant::now();

        state
            .seen_events
            .retain(|_, first_seen| now.duration_since(*first_seen) <= DEDUP_WINDOW);

        if state.seen_events.contains_key(&event.event_id) {
            return None;
        }

        state.seen_events.insert(event.event_id, now);

        // ---------------------------------------------------------
        // 2. Get correlation state for this username
        // ---------------------------------------------------------

        let failures = state.failures.entry(event.username.clone()).or_default();

        // ---------------------------------------------------------
        // 3. Failed login
        // ---------------------------------------------------------

        if event.kind == EventKind::FailedLogin {
            *failures.entry(event.timestamp).or_insert(0) += 1;

            /*
             * Remove failures older than the 60-second window
             * relative to this event's timestamp.
             */
            evict_old_failures(failures, event.timestamp);

            return None;
        }

        // ---------------------------------------------------------
        // 4. Login success
        // ---------------------------------------------------------

        /*
         * Correlation is based on the event's REAL timestamp,
         * not arrival order.
         */
        evict_old_failures(failures, event.timestamp);

        /*
         * Only failures at or before the success timestamp can
         * satisfy "failed logins followed by success".
         */
        let failed_count = failures
            .range(..=event.timestamp)
            .map(|(_, count)| *count)
            .sum::<usize>();

        if failed_count < 5 {
            return None;
        }

        // Pattern detected.
        let alert = Alert {
            username: event.username,
            failed_count,
        };

        /*
         * Consume the current correlation state so the same
         * failures don't generate an alert for every success.
         */
        failures.clear();

        Some(alert)
    }
}

/// Remove failed-login events older than 60 seconds.
///
/// The 60-second boundary is inclusive:
/// exactly 60 seconds old is still considered valid.
fn evict_old_failures(failures: &mut BTreeMap<Instant, usize>, reference_time: Instant) {
    let Some(cutoff) = reference_time.checked_sub(CORRELATION_WINDOW) else {
        return;
    };

    /*
     * BTreeMap is ordered by timestamp, so only timestamps
     * before the cutoff need to be removed.
     */
    let old_timestamps: Vec<Instant> = failures
        .range(..cutoff)
        .map(|(timestamp, _)| *timestamp)
        .collect();

    for timestamp in old_timestamps {
        failures.remove(&timestamp);
    }
}

#[tokio::main]
async fn main() {
    let engine = Arc::new(CorrelationEngine::new());
    let now = Instant::now();

    // -------------------------------------------------------------
    // Example 1: Alice -> 5 failures + success -> Alert
    // -------------------------------------------------------------

    for i in 0..5 {
        let result = engine
            .ingest(LogEvent {
                event_id: i + 1,
                username: "alice".to_string(),
                kind: EventKind::FailedLogin,
                timestamp: now + Duration::from_secs(i),
            })
            .await;

        assert!(result.is_none());
    }

    let alert = engine
        .ingest(LogEvent {
            event_id: 6,
            username: "alice".to_string(),
            kind: EventKind::LoginSuccess,
            timestamp: now + Duration::from_secs(10),
        })
        .await;

    println!("Alice alert: {alert:?}");

    // -------------------------------------------------------------
    // Example 2: duplicate event ID -> ignored
    // -------------------------------------------------------------

    let duplicate = engine
        .ingest(LogEvent {
            event_id: 6,
            username: "alice".to_string(),
            kind: EventKind::LoginSuccess,
            timestamp: now + Duration::from_secs(10),
        })
        .await;

    println!("Duplicate result: {duplicate:?}");

    assert!(duplicate.is_none());

    // -------------------------------------------------------------
    // Example 3: Alice and Bob processed concurrently
    // -------------------------------------------------------------

    let alice_engine = Arc::clone(&engine);
    let bob_engine = Arc::clone(&engine);

    let alice_task = tokio::spawn(async move {
        for i in 0..5 {
            alice_engine
                .ingest(LogEvent {
                    event_id: 100 + i,
                    username: "alice-concurrent".to_string(),
                    kind: EventKind::FailedLogin,
                    timestamp: now + Duration::from_secs(i),
                })
                .await;
        }

        alice_engine
            .ingest(LogEvent {
                event_id: 200,
                username: "alice-concurrent".to_string(),
                kind: EventKind::LoginSuccess,
                timestamp: now + Duration::from_secs(10),
            })
            .await
    });

    let bob_task = tokio::spawn(async move {
        for i in 0..5 {
            bob_engine
                .ingest(LogEvent {
                    event_id: 300 + i,
                    username: "bob".to_string(),
                    kind: EventKind::FailedLogin,
                    timestamp: now + Duration::from_secs(i),
                })
                .await;
        }

        bob_engine
            .ingest(LogEvent {
                event_id: 400,
                username: "bob".to_string(),
                kind: EventKind::LoginSuccess,
                timestamp: now + Duration::from_secs(10),
            })
            .await
    });

    let alice_alert = alice_task.await.unwrap();
    let bob_alert = bob_task.await.unwrap();

    println!("Concurrent Alice alert: {alice_alert:?}");
    println!("Concurrent Bob alert: {bob_alert:?}");
}

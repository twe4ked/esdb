use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use sled::{Config, Db};
use uuid::Uuid;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const DEFAULT_LIMIT: usize = 1000;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct NewEvent {
    pub aggregate_sequence: u64,
    pub aggregate_type: String,
    pub event_type: String,
    pub body: JsonValue,
    pub metadata: JsonValue,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Event {
    pub sequence: u64,
    pub aggregate_sequence: u64,
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub aggregate_type: String,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub body: JsonValue,
    pub metadata: JsonValue,
}

impl Event {
    fn from_new_event(
        new_event: NewEvent,
        aggregate_id: Uuid,
        sequence: u64,
        event_id: Uuid,
    ) -> Self {
        let NewEvent {
            aggregate_sequence,
            aggregate_type,
            event_type,
            body,
            metadata,
        } = new_event;

        Event {
            sequence,
            aggregate_sequence,
            event_id,
            aggregate_id,
            aggregate_type,
            event_type,
            created_at: Utc::now(),
            body,
            metadata,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
struct EventId(Uuid);

#[derive(Clone)]
pub struct EventStore {
    db: Db,
    sequences: Sequences,
}

impl EventStore {
    pub fn new() -> sled::Result<Self> {
        let db = Config::default().temporary(true).open()?;
        Ok(Self::new_with_db(db))
    }

    pub fn new_with_db(db: Db) -> Self {
        Self {
            db,
            sequences: Sequences::new(),
        }
    }

    // TODO: This should take a list of events and sink them as part of the same transaction.
    // The API should be designed in a way that makes it impossible to sink events to different
    // aggregates.
    pub fn sink(&self, new_event: NewEvent, aggregate_id: Uuid) -> sled::Result<()> {
        let aggregates = self.db.open_tree("aggregates")?;
        let events = self.db.open_tree("events")?;

        let sequence = self.sequences.generate(&self.db)?;

        // KEY: aggregate_id + aggregate_sequence
        let mut aggregates_key = aggregate_id.as_bytes().to_vec();
        aggregates_key.extend_from_slice(&new_event.aggregate_sequence.to_be_bytes());
        aggregates
            // The compare_and_swap here ensure the aggregate_id + aggregate_sequence is unique so we
            // can be sure we don't have a stale aggregate.
            .compare_and_swap(
                &aggregates_key,
                None as Option<&[u8]>,
                Some(serde_json::to_vec(&sequence.value).unwrap()),
            )?
            .expect("stale aggregate");

        let event_id = Uuid::new_v4(); // TODO: Ensure uniqueness
        let event = Event::from_new_event(new_event, aggregate_id, sequence.value, event_id);

        // KEY: sequence
        events.insert(
            &sequence.value.to_be_bytes(),
            serde_json::to_vec(&event).unwrap(),
        )?;

        Ok(())
    }

    pub fn for_aggregate(&self, aggregate_id: Uuid) -> sled::Result<Vec<Event>> {
        let events = self.db.open_tree("events")?;
        let aggregates = self.db.open_tree("aggregates")?;

        Ok(aggregates
            .scan_prefix(&aggregate_id.as_bytes())
            .filter_map(|e| e.ok())
            .map(|(_, s)| -> u64 { serde_json::from_slice(&s).expect("decode error") })
            .map(|s| events.get(&s.to_be_bytes()).unwrap())
            .map(|e| -> Event { serde_json::from_slice(&e.unwrap()).unwrap() })
            .collect())
    }

    pub fn after(&self, sequence: u64, limit: Option<usize>) -> sled::Result<Vec<Event>> {
        // We want events _after_ this sequence.
        let sequence = sequence + 1;

        let limit = limit.unwrap_or(DEFAULT_LIMIT);

        // After this call, sequences can't start being removed, unless they're already in the
        // process of being removed, which is okay because they must already be finished.
        self.sequences.start_reading();

        // Fetch the events and convert them info `Event`s. N.b. we collect the events into a Vec
        // to finish reading them rather than returning an iterator here.
        let mut events: Vec<_> = self
            .db
            .open_tree("events")?
            .range(sequence.to_be_bytes()..)
            .map(|e| e.unwrap())
            .map(|(_, e)| -> Event { serde_json::from_slice(&e).expect("decode error") })
            .take(limit)
            .collect();

        // Read any in-flight sequences that are still in-flight and find the min in-flight
        // sequence.
        let min_in_flight_sequence = self.sequences.min_in_flight_sequence();

        // We've now read the events (and got the minimum in-flight sequence) we can allow removing
        // sequences.
        self.sequences.finished_reading();

        if let Some(s) = min_in_flight_sequence {
            // If we have any in-flight sequences, we only want to retain events where the sequence
            // is earlier than the min in-flight sequence.
            events.retain(|e| e.sequence < s);
        }

        Ok(events)
    }
}

#[derive(Clone)]
struct Sequences {
    in_flight_sequences: Arc<RwLock<HashSet<u64>>>,
    finished_sequences: Arc<RwLock<HashSet<u64>>>,
    readers_count: Arc<AtomicU64>,
    generate_sequence_lock: Arc<Mutex<()>>,
}

impl Sequences {
    fn new() -> Self {
        Self {
            // List of in-flight sequences
            in_flight_sequences: Arc::new(RwLock::new(HashSet::new())),
            // List of sequences that have been marked as "finished" ready for removal
            finished_sequences: Arc::new(RwLock::new(HashSet::new())),
            // Number of threads currently reading. This is used to ensure we don't remove
            // in-flight sequences while threads might be using them.
            readers_count: Arc::new(AtomicU64::new(0)),
            // Used to ensure newly generated sequences is marked as in-flight before they're used.
            generate_sequence_lock: Arc::new(Mutex::new(())),
        }
    }

    fn min_in_flight_sequence(&self) -> Option<u64> {
        self.in_flight_sequences
            .read()
            .unwrap()
            .iter()
            .min()
            .copied()
    }

    fn start_reading(&self) {
        // Lock removing of in-flight sequences while we fetch events
        self.readers_count.fetch_add(1, Ordering::SeqCst);
    }

    fn mark_sequence_in_flight(&self, sequence: u64) {
        self.in_flight_sequences.write().unwrap().insert(sequence);
    }

    fn mark_sequence_finished(&self, sequence: u64) {
        self.finished_sequences.write().unwrap().insert(sequence);
    }

    fn finished_reading(&self) {
        self.readers_count.fetch_sub(1, Ordering::SeqCst);

        // If there are no other readers we're safe to remove in-flight sequences
        if self.readers_count.load(Ordering::SeqCst) == 0 {
            // Read the finished sequences first
            let mut finished_sequences = self.finished_sequences.write().unwrap();

            // Remove any sequences that are no longer in flight
            let mut in_flight_sequences = self.in_flight_sequences.write().unwrap();
            for sequence in finished_sequences.drain() {
                in_flight_sequences.remove(&sequence);
            }
        }
    }

    /// When generating a sequence we need to do it in a lock so that we can be sure that no other
    /// IDs get generated before we mark the new sequence as in-flight.
    fn generate(&self, db: &Db) -> sled::Result<NewSequence> {
        match self.generate_sequence_lock.lock() {
            Ok(_) => {
                let sequence = db.generate_id()?;
                // Start sequence at 1.
                let sequence = sequence + 1;
                self.mark_sequence_in_flight(sequence);
                Ok(NewSequence::new(sequence, &self))
            }
            Err(e) => panic!("{}", e),
        }
    }
}

struct NewSequence<'a> {
    value: u64,
    sequences: &'a Sequences,
}

impl<'a> NewSequence<'a> {
    fn new(value: u64, sequences: &'a Sequences) -> Self {
        Self { value, sequences }
    }
}

impl Drop for NewSequence<'_> {
    fn drop(&mut self) {
        self.sequences.mark_sequence_finished(self.value);
    }
}

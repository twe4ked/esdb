use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use sled::{Config, Db};
use uuid::Uuid;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

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
                Some(serde_json::to_vec(&sequence).unwrap()),
            )?
            .expect("stale aggregate");

        let event_id = Uuid::new_v4(); // TODO: Ensure uniqueness
        let event = Event::from_new_event(new_event, aggregate_id, sequence, event_id);

        // KEY: sequence
        events.insert(&sequence.to_be_bytes(), serde_json::to_vec(&event).unwrap())?;

        self.sequences.mark_sequence_finished(sequence);

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

    pub fn after(&self, sequence: u64) -> sled::Result<Vec<Event>> {
        // We want events _after_ this sequence.
        let sequence = sequence + 1;

        self.sequences.start_reading();

        // Fetch the events and convert them info `Event`s
        let events = self
            .db
            .open_tree("events")?
            .range(sequence.to_be_bytes()..(sequence + 1000).to_be_bytes())
            .map(|e| e.unwrap())
            .map(|(_, e)| -> Event { serde_json::from_slice(&e).expect("decode error") });

        // Read any in-flight sequences that are still in-flight and find the min in-flight
        // sequence
        let min_in_flight_sequence = self.sequences.min_in_flight_sequence();

        // We've now read the events and any in-flight sequences so we can allow removing
        // in-flight sequences again.
        self.sequences.finished_reading();

        if let Some(s) = min_in_flight_sequence {
            // If we have any in-flight sequences, we want to filter out any events that are after
            // the min in-flight sequence
            Ok(events.filter(|e| e.sequence < s).collect())
        } else {
            Ok(events.collect())
        }
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
        self.try_remove_finished_in_flight_sequences();

        // While we have the "removing" lock we know that no other threads are removing
        // sequences. This means we're safe to increment "readers_count"
        // which will prevent other threads from removing in-flight sequences while we're
        // reading.
        //
        // Lock removing of in-flight sequences while we fetch events
        self.readers_count.fetch_add(1, Ordering::SeqCst);
    }

    fn try_remove_finished_in_flight_sequences(&self) {
        // Read the finished sequences first
        let finished_sequences = self.finished_sequences.read().unwrap();

        // If there are no other readers we're safe to remove in-flight sequences
        if self.readers_count.load(Ordering::SeqCst) == 0 {
            let mut w = self.in_flight_sequences.write().unwrap();

            // Remove any sequences that are no longer in flight
            for sequence in finished_sequences.iter() {
                w.remove(&sequence);
            }
        }
    }

    fn mark_sequence_in_flight(&self, sequence: u64) {
        self.in_flight_sequences.write().unwrap().insert(sequence);
    }

    fn mark_sequence_finished(&self, sequence: u64) {
        self.finished_sequences.write().unwrap().insert(sequence);
    }

    fn finished_reading(&self) {
        self.readers_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// When generating a sequence we need to do it in a lock so that we can be sure that no other
    /// IDs get generated before we mark the new sequence as in-flight.
    fn generate(&self, db: &Db) -> sled::Result<u64> {
        match self.generate_sequence_lock.lock() {
            Ok(_) => {
                let sequence = db.generate_id()?;
                // Start sequence at 1.
                let sequence = sequence + 1;
                self.mark_sequence_in_flight(sequence);
                Ok(sequence)
            }
            Err(e) => panic!("{}", e),
        }
    }
}

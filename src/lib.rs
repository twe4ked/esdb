use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use sled::{Config, Db};
use uuid::Uuid;

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

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
    in_flight_sequences: Arc<RwLock<HashSet<u64>>>,
    in_flight_sequences_finished: Arc<RwLock<HashSet<u64>>>,
}

fn to_key<T>(v: Vec<T>) -> [T; 24] {
    use std::convert::TryInto;

    let boxed_slice = v.into_boxed_slice();
    let boxed_array: Box<[T; 24]> = match boxed_slice.try_into() {
        Ok(ba) => ba,
        Err(o) => panic!("Expected a Vec of length {} but it was {}", 24, o.len()),
    };
    *boxed_array
}

impl EventStore {
    pub fn new() -> sled::Result<Self> {
        let db = Config::default().temporary(true).open()?;
        Ok(Self::new_with_db(db))
    }

    pub fn new_with_db(db: Db) -> Self {
        Self {
            db,
            in_flight_sequences: Arc::new(RwLock::new(HashSet::new())),
            in_flight_sequences_finished: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    // TODO: This should take a list of events and sink them as part of the same transaction.
    // The API should be designed in a way that makes it impossible to sink events to different
    // aggregates.
    pub fn sink(&self, new_event: NewEvent, aggregate_id: Uuid) -> sled::Result<()> {
        let aggregates = self.db.open_tree("aggregates")?;
        let events = self.db.open_tree("events")?;

        // Start sequence at 1.
        let sequence = self.db.generate_id()? + 1;

        self.mark_sequence_in_flight(sequence);

        // KEY: aggregate_id + aggregate_sequence
        let aggregates_key: Vec<u8> = aggregate_id
            .as_bytes()
            .clone()
            .iter()
            .chain(new_event.aggregate_sequence.to_be_bytes().iter())
            .copied()
            .collect();
        // The compare_and_swap here ensure the aggregate_id + aggregate_sequence is unique so we
        // can be sure we don't have a stale aggregate.
        aggregates
            .compare_and_swap(
                &to_key(aggregates_key),
                None as Option<&[u8]>,
                Some(serde_json::to_vec(&sequence).unwrap()),
            )?
            .expect("stale aggregate");

        let event_id = Uuid::new_v4(); // TODO: Ensure uniqueness
        let event = Event::from_new_event(new_event, aggregate_id, sequence, event_id);

        // KEY: sequence
        events.insert(&sequence.to_be_bytes(), serde_json::to_vec(&event).unwrap())?;

        self.mark_sequence_finished(sequence);

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

        // Remove any sequences that are no longer in flight
        self.remove_finished_in_flight_sequences();
        // Then read any sequences that are still in-flight,
        let in_flight_sequences_before = self.in_flight_sequences.read().unwrap().clone();

        // Fetch the events and convert them info `Event`s
        let events = self
            .db
            .open_tree("events")?
            .range(sequence.to_be_bytes()..(sequence + 1000).to_be_bytes())
            .map(|e| e.unwrap())
            .map(|(_, e)| -> Event { serde_json::from_slice(&e).expect("decode error") });

        // Read any in-flight sequences that are still in-flight. N.b. we're not removing any
        // sequences that were in-flight in case any were added while we were reading events.
        let in_flight_sequences_after = self.in_flight_sequences.read().unwrap();

        // Find the min in-flight sequence
        let min_in_flight_sequence = in_flight_sequences_before
            .iter()
            .chain(in_flight_sequences_after.iter())
            .min()
            .copied();

        if let Some(s) = min_in_flight_sequence {
            // If we have any in-flight sequences we want to filter out any events that are after
            // the min in-flight sequence
            Ok(events.filter(|e| e.sequence < s).collect())
        } else {
            Ok(events.collect())
        }
    }

    fn remove_finished_in_flight_sequences(&self) {
        for sequence in self.in_flight_sequences_finished.read().unwrap().iter() {
            let mut w = self.in_flight_sequences.write().unwrap();
            w.remove(&sequence);
        }
    }

    fn mark_sequence_in_flight(&self, sequence: u64) {
        let mut w = self.in_flight_sequences.write().unwrap();
        w.insert(sequence);
    }

    fn mark_sequence_finished(&self, sequence: u64) {
        let mut w = self.in_flight_sequences_finished.write().unwrap();
        w.insert(sequence);
    }
}

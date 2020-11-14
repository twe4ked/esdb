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
    pub fn new() -> Self {
        Self::new_with_db(Config::default().temporary(true).open().unwrap())
    }

    pub fn new_with_db(db: Db) -> Self {
        Self {
            db,
            in_flight_sequences: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    // TODO: This should take a list of events and sink them as part of the same transaction.
    // The API should be designed in a way that makes it impossible to sink events to different
    // aggregates.
    pub fn sink(&self, new_event: NewEvent, aggregate_id: Uuid) -> std::io::Result<()> {
        let aggregates = self.db.open_tree("aggregates").unwrap();
        let sequences = self.db.open_tree("sequences").unwrap();
        let events = self.db.open_tree("events").unwrap();

        let event_id = Uuid::new_v4();

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
                Some(serde_json::to_vec(&EventId(event_id)).unwrap()),
            )
            .unwrap()
            .expect("stale aggregate");

        fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
            use std::convert::TryInto;

            let number = match old {
                Some(bytes) => {
                    let array: [u8; 8] = bytes.try_into().unwrap();
                    let number = u64::from_be_bytes(array);
                    number + 1
                }
                None => 1,
            };

            Some(number.to_be_bytes().to_vec())
        }

        let sequence = self.db.open_tree("sequence").unwrap();
        let event_sequence = sequence
            .update_and_fetch("sequence", increment)
            .unwrap()
            .unwrap();

        // KEY: sequence
        sequences
            .insert(
                &event_sequence,
                serde_json::to_vec(&EventId(event_id)).unwrap(),
            )
            .unwrap();

        let sequence = {
            use std::convert::TryInto;
            let array: [u8; 8] = (*event_sequence).try_into().unwrap();
            u64::from_be_bytes(array)
        };

        {
            // TODO: This write blocks, so we want to swap this out with something more performant.
            let mut w = self.in_flight_sequences.write().unwrap();
            w.insert(sequence);
        }

        let event = Event::from_new_event(new_event, aggregate_id, sequence, event_id);

        // KEY: event_id
        events
            .insert(
                &event_id.as_bytes().clone(),
                serde_json::to_vec(&event).unwrap(),
            )
            .unwrap();

        {
            // TODO: This write blocks, so we want to swap this out with something more performant.
            let mut w = self.in_flight_sequences.write().unwrap();
            w.remove(&sequence);
        }

        Ok(())
    }

    pub fn for_aggregate(&self, aggregate_id: Uuid) -> Vec<Event> {
        let events = self.db.open_tree("events").unwrap();
        let aggregates = self.db.open_tree("aggregates").unwrap();

        let event_ids = aggregates.scan_prefix(&aggregate_id.as_bytes());

        event_ids
            .map(|event_id| -> EventId {
                let (_k, event_id) = event_id.unwrap();
                serde_json::from_slice(&event_id).unwrap()
            })
            .map(|event_id| events.get(event_id.0.as_bytes()).unwrap())
            .map(|e| -> Event { serde_json::from_slice(&e.unwrap()).unwrap() })
            .collect()
    }

    pub fn after(&self, sequence: u64) -> Vec<Event> {
        let events = self.db.open_tree("events").unwrap();
        let sequences = self.db.open_tree("sequences").unwrap();

        // We want events _after_ this sequence.
        let sequence = sequence + 1;

        let event_ids = sequences.range(sequence.to_be_bytes()..(sequence + 1000).to_be_bytes());

        let in_flight_sequences = self.in_flight_sequences.read().unwrap();

        event_ids
            // Turn event IDs into EventId(Uuid)
            .map(|event_id| -> EventId {
                let (_k, event_id) = event_id.unwrap();
                serde_json::from_slice(&event_id).unwrap()
            })
            // Turn into Uuid into bytes and look up Events (returned as JSON)
            .map(|event_id| events.get(event_id.0.as_bytes()).unwrap())
            // Decode into Events
            .filter_map(|e| e)
            .map(|e| -> Event { serde_json::from_slice(&e).expect("decode error") })
            // Filter
            .filter(|e| in_flight_sequences.iter().any(|ifs| ifs >= &e.sequence))
            .collect()
    }
}

impl Default for EventStore {
    fn default() -> Self {
        Self::new()
    }
}

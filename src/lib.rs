use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use sled::transaction::Transactional;
use sled::{Config, Db};
use uuid::Uuid;

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

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
struct EventId(Uuid);

pub struct EventStore {
    db: Db,
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
        let db = Config::default().temporary(true).open().unwrap();

        Self { db }
    }

    pub fn new_with_db(db: Db) -> Self {
        Self { db }
    }

    // TODO: This should take a list of events and sink them as part of the same transaction.
    // The API should be designed in a way that makes it impossible to sink events to different
    // aggregates.
    pub fn sink(
        &self,
        new_event: NewEvent,
        aggregate_id: Uuid,
    ) -> sled::transaction::TransactionResult<()> {
        let aggregates = self.db.open_tree("aggregates").unwrap();
        let sequences = self.db.open_tree("sequences").unwrap();
        let events = self.db.open_tree("events").unwrap();

        (&aggregates, &sequences, &events).transaction(|(aggregates, sequences, events)| {
            let event_id = Uuid::new_v4();

            // KEY: aggregate_id + aggregate_sequence
            //
            // We need to ensure this key is unique otherwise we have a "stale aggregate".
            let aggregates_key: Vec<u8> = aggregate_id
                .as_bytes()
                .clone()
                .iter()
                .chain(new_event.aggregate_sequence.to_be_bytes().iter())
                .copied()
                .collect();
            aggregates.insert(
                &to_key(aggregates_key),
                serde_json::to_vec(&EventId(event_id)).unwrap(),
            )?;

            // NOTE: Starts at 0 so we're adding 1.
            let event_sequence = sequences.generate_id().unwrap() + 1;

            // KEY: sequence
            sequences.insert(
                &event_sequence.to_be_bytes(),
                serde_json::to_vec(&EventId(event_id)).unwrap(),
            )?;

            let event = Event {
                sequence: event_sequence,
                aggregate_sequence: new_event.aggregate_sequence.clone(),
                event_id,
                aggregate_id: aggregate_id,
                aggregate_type: new_event.aggregate_type.clone(),
                event_type: new_event.event_type.clone(),
                created_at: Utc::now(),
                body: new_event.body.clone(),
                metadata: new_event.metadata.clone(),
            };

            // KEY: event_id
            events.insert(
                &event_id.as_bytes().clone(),
                serde_json::to_vec(&event).unwrap(),
            )?;

            Ok(())
        })?;

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

    // TODO: We need to ensure that events are never read out of order here.
    //
    // For example, if events with sequences 1 and 2 are being written concurrently and event 2
    // finishes writing first, we need to ensure that we don't see event 2 here before event 1.
    pub fn after(&self, sequence: u64) -> Vec<Event> {
        let events = self.db.open_tree("events").unwrap();
        let sequences = self.db.open_tree("sequences").unwrap();

        // We want events _after_ this sequence.
        let sequence = sequence + 1;

        let event_ids = sequences.range(sequence.to_be_bytes()..(sequence + 1000).to_be_bytes());

        event_ids
            .map(|event_id| {
                let (_k, event_id) = event_id.unwrap();
                let e: EventId = serde_json::from_slice(&event_id).unwrap();
                e
            })
            .map(|event_id| events.get(event_id.0.as_bytes()).unwrap())
            .map(|x| {
                let e: Event = serde_json::from_slice(&x.unwrap()).unwrap();
                e
            })
            .collect()
    }
}

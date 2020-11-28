use chrono::prelude::*;
use persy::{IndexIter, OpenOptions, PRes, Persy, PersyId, Value, ValueMode};
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use thiserror::Error;
use uuid::Uuid;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const DEFAULT_LIMIT: usize = 1000;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct NewEvent {
    pub aggregate_sequence: u64,
    pub event_type: String,
    pub body: JsonValue,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Event {
    // Events only contain a sequence when they're returned from `after`
    pub sequence: Option<u64>,
    pub aggregate_sequence: u64,
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub body: JsonValue,
}

impl Event {
    fn from_event_data(event_data: EventValue, sequence: Option<u64>) -> Self {
        let EventValue {
            aggregate_sequence,
            event_type,
            body,
            event_id,
            aggregate_id,
            created_at,
        } = event_data;

        Self {
            aggregate_sequence,
            event_id,
            sequence,
            aggregate_id,
            event_type,
            created_at,
            body,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
struct EventValue {
    aggregate_sequence: u64,
    event_id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    created_at: DateTime<Utc>,
    body: JsonValue,
}

impl EventValue {
    fn from_new_event(new_event: NewEvent, aggregate_id: Uuid, event_id: Uuid) -> Self {
        let NewEvent {
            aggregate_sequence,
            event_type,
            body,
        } = new_event;

        Self {
            aggregate_sequence,
            event_id,
            aggregate_id,
            event_type,
            created_at: Utc::now(),
            body,
        }
    }
}

#[derive(Clone)]
pub struct EventStore {
    persy: Persy,
    uuid_generator: Arc<dyn UuidGenerator>,
    sequence: Arc<AtomicU64>,
}

#[derive(Error, Debug, PartialEq)]
pub enum SinkError {
    #[error("Stale aggregate")]
    StaleAggregate,
    #[error("Event ID conflict")]
    EventIdConflict,
}

trait UuidGenerator: Sync + Send {
    fn generate(&self) -> Uuid;
}

struct UuidGeneratorV4;

impl UuidGenerator for UuidGeneratorV4 {
    fn generate(&self) -> Uuid {
        Uuid::new_v4()
    }
}

fn init_db(persy: &Persy) -> PRes<()> {
    let mut tx = persy.begin()?;

    tx.create_segment("events")?;
    tx.create_index::<u128, PersyId>("aggregate", ValueMode::CLUSTER)?;
    tx.create_index::<String, PersyId>("aggregate_unique", ValueMode::EXCLUSIVE)?;
    tx.create_index::<u128, PersyId>("event_id_unique", ValueMode::EXCLUSIVE)?;
    tx.create_index::<u64, PersyId>("sequence", ValueMode::EXCLUSIVE)?;

    let prepared = tx.prepare()?;
    prepared.commit()?;

    Ok(())
}

impl EventStore {
    pub fn new_temporary() -> PRes<Self> {
        let persy = OpenOptions::new()
            .create(true)
            .prepare_with(init_db)
            .memory()?;
        Ok(Self::new_with_persy(persy))
    }

    pub fn new_persisted(path: &Path) -> PRes<Self> {
        let persy = OpenOptions::new()
            .create(true)
            .prepare_with(init_db)
            .open(path)?;
        Ok(Self::new_with_persy(persy))
    }

    fn new_with_persy(persy: Persy) -> Self {
        Self {
            persy,
            uuid_generator: Arc::new(UuidGeneratorV4),
            // TODO: Find current sequence
            sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn sink(&self, new_events: Vec<NewEvent>, aggregate_id: Uuid) -> PRes<()> {
        let mut tx = self.persy.begin()?;

        for new_event in new_events.clone() {
            let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

            let event_id = self.uuid_generator.generate();
            let aggregate_sequence = new_event.aggregate_sequence;

            let event = EventValue::from_new_event(new_event, aggregate_id, event_id);
            let id = tx.insert("events", &serde_json::to_vec(&event).unwrap())?;

            let optimistic_lock = format!("{}:{}", aggregate_id, aggregate_sequence);

            // We can't handle the stale aggregate error as the caller will need to reload the
            // aggregate and try again.
            tx.put::<String, PersyId>("aggregate_unique", optimistic_lock, id.clone())?;

            // TODO: We can attempt to handle these errors internally
            tx.put::<u128, PersyId>("event_id_unique", event_id.as_u128(), id.clone())?;
            tx.put::<u128, PersyId>("aggregate", aggregate_id.as_u128(), id.clone())?;
            tx.put::<u64, PersyId>("sequence", sequence, id)?;
        }

        let prepared = tx.prepare()?;
        prepared.commit()?;

        Ok(())
    }

    pub fn for_aggregate(&self, aggregate_id: Uuid) -> PRes<Vec<Event>> {
        let read_id = self
            .persy
            .get::<u128, PersyId>("aggregate", &aggregate_id.as_u128())?;

        if let Some(value) = read_id {
            match value {
                Value::SINGLE(id) => {
                    let e = self.persy.read("events", &id)?;
                    let event_data: EventValue = serde_json::from_slice(&e.unwrap()).unwrap();
                    Ok(vec![Event::from_event_data(event_data, None)])
                }
                Value::CLUSTER(_) => todo!("handle multiple events"),
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn after(&self, sequence: u64, limit: Option<usize>) -> PRes<Vec<Event>> {
        // We want events _after_ this sequence.
        let sequence = sequence + 1;
        let limit = limit.unwrap_or(DEFAULT_LIMIT);

        let iter: IndexIter<u64, PersyId> = self.persy.range("sequence", sequence..)?;
        let iter = iter.take(limit).map(|(sequence, value)| match value {
            Value::SINGLE(id) => self.persy.read("events", &id).map(|e| (sequence, e)),
            Value::CLUSTER(_) => unreachable!("only one event per sequence"),
        });

        itertools::process_results(iter, |iter| {
            iter.map(|(sequence, e)| {
                let event_data: EventValue = serde_json::from_slice(&e.unwrap()).unwrap();
                Event::from_event_data(event_data, Some(sequence))
            })
            .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn basic() {
        let event_store = EventStore::new_temporary().unwrap();

        let aggregate_id = Uuid::new_v4();
        let event = NewEvent {
            aggregate_sequence: 1,
            event_type: "foo_bar".to_string(),
            body: json!({"foo": "bar"}),
        };

        event_store.sink(vec![event], aggregate_id.clone()).unwrap();

        {
            let events = event_store.after(0, None).unwrap();
            let event = events.first().unwrap();
            assert_eq!(event.sequence, Some(1));
            assert_eq!(event.aggregate_sequence, 1);
            assert_eq!(event.aggregate_id, aggregate_id);
            assert_eq!(event.event_type, "foo_bar".to_string());
            assert_eq!(event.body, json!({"foo": "bar"}));
        }

        {
            let events = event_store.for_aggregate(aggregate_id).unwrap();
            let event = events.first().unwrap();
            assert_eq!(event.sequence, None);
            assert_eq!(event.aggregate_sequence, 1);
            assert_eq!(event.aggregate_id, aggregate_id);
            assert_eq!(event.event_type, "foo_bar".to_string());
            assert_eq!(event.body, json!({"foo": "bar"}));
        }
    }

    #[test]
    fn sink_stale_aggregate() {
        let event_store = EventStore::new_temporary().unwrap();

        let aggregate_id = Uuid::new_v4();
        let event = NewEvent {
            aggregate_sequence: 1,
            event_type: String::new(),
            body: json!({}),
        };

        let sink_1_result = event_store.sink(vec![event.clone()], aggregate_id.clone());
        assert!(sink_1_result.is_ok());

        // The second sink will fail because the aggregate_sequence has already been used by this
        // aggregate.
        let sink_2_result = event_store.sink(vec![event], aggregate_id.clone());
        // TODO: Return SinkError::StaleAggregate
        assert!(sink_2_result.is_err());
    }

    #[test]
    fn sink_duplicate_event_id() {
        let mut event_store = EventStore::new_temporary().unwrap();

        // Override the event UUID generator so we can make it generate a duplicate
        struct FakeUuidGenerator;
        impl UuidGenerator for FakeUuidGenerator {
            fn generate(&self) -> Uuid {
                Uuid::parse_str("0436430c-2b02-624c-2032-570501212b57").unwrap()
            }
        }
        event_store.uuid_generator = Arc::new(FakeUuidGenerator);

        let event = NewEvent {
            aggregate_sequence: 1,
            event_type: String::new(),
            body: json!({}),
        };

        let sink_1_result = event_store.sink(vec![event.clone()], Uuid::new_v4());
        assert!(sink_1_result.is_ok());

        let sink_2_result = event_store.sink(vec![event], Uuid::new_v4());
        // TODO: Return SinkError::EventIdConflict
        assert!(sink_2_result.is_err());
    }
}

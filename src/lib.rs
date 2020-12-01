use chrono::prelude::*;
use persy::{IndexIter, OpenOptions, PRes, Persy, PersyError, PersyId, Value, ValueMode};
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use thiserror::Error;
use uuid::Uuid;

use std::path::Path;
use std::sync::Arc;

mod sequences;

use crate::sequences::Sequences;

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

    fn from_slice(e: &[u8]) -> Self {
        let event_data: EventValue = serde_json::from_slice(&e).unwrap();
        Self::from_event_data(event_data, None)
    }

    fn from_slice_and_sequence(e: &[u8], sequence: Option<u64>) -> Self {
        let event_data: EventValue = serde_json::from_slice(&e).unwrap();
        Self::from_event_data(event_data, sequence)
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
    sequences: Sequences,
}

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Stale aggregate: {0}")]
    StaleAggregate(String),
    #[error(transparent)]
    StorageError(#[from] PersyError),
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

const IDX_AGGREGATE_UNIQUE: &'static str = "aggregate_unique";
const IDX_AGGREGATE: &'static str = "aggregate";
const IDX_EVENT_ID_UNIQUE: &'static str = "event_id_unique";
const IDX_SEQUENCE: &'static str = "sequence";

fn init_db(persy: &Persy) -> PRes<()> {
    let mut tx = persy.begin()?;

    tx.create_segment("events")?;
    tx.create_index::<u128, PersyId>(IDX_AGGREGATE, ValueMode::CLUSTER)?;
    tx.create_index::<String, PersyId>(IDX_AGGREGATE_UNIQUE, ValueMode::EXCLUSIVE)?;
    tx.create_index::<u128, PersyId>(IDX_EVENT_ID_UNIQUE, ValueMode::EXCLUSIVE)?;
    tx.create_index::<u64, PersyId>(IDX_SEQUENCE, ValueMode::EXCLUSIVE)?;

    let prepared = tx.prepare()?;
    prepared.commit()?;

    Ok(())
}

fn persy_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.create(true).prepare_with(init_db);
    options
}

impl EventStore {
    pub fn new_temporary() -> PRes<Self> {
        let persy = persy_options().memory()?;
        Ok(Self::new_with_persy(persy))
    }

    pub fn new_persisted(path: &Path) -> PRes<Self> {
        let persy = persy_options().open(path)?;
        Ok(Self::new_with_persy(persy))
    }

    fn new_with_persy(persy: Persy) -> Self {
        Self {
            persy,
            uuid_generator: Arc::new(UuidGeneratorV4),
            sequences: Sequences::new(),
        }
    }

    pub fn sink(&self, new_events: Vec<NewEvent>, aggregate_id: Uuid) -> Result<(), SinkError> {
        let mut tx = self.persy.begin()?;

        // We need to keep the in-flight sequences around until _after_ the commit.
        let mut in_flight_sequences = Vec::new();

        for new_event in new_events.clone() {
            let sequence = self.sequences.generate();

            let event_id = self.uuid_generator.generate();
            let aggregate_sequence = new_event.aggregate_sequence;

            let event = EventValue::from_new_event(new_event, aggregate_id, event_id);
            let id = tx.insert("events", &serde_json::to_vec(&event).unwrap())?;

            let optimistic_lock = format!("{}:{}", aggregate_id, aggregate_sequence);

            // We can't handle the stale aggregate error as the caller will need to reload the
            // aggregate and try again.
            tx.put(IDX_AGGREGATE_UNIQUE, optimistic_lock, id.clone())?;

            // TODO: We can attempt to handle these errors internally
            tx.put(IDX_EVENT_ID_UNIQUE, event_id.as_u128(), id.clone())?;
            tx.put(IDX_AGGREGATE, aggregate_id.as_u128(), id.clone())?;
            tx.put(IDX_SEQUENCE, sequence.value, id)?;

            in_flight_sequences.push(sequence);
        }

        let prepared = tx.prepare().map_err(|e| match e {
            PersyError::IndexDuplicateKey(index, value) if index == "aggregate_unique" => {
                SinkError::StaleAggregate(value)
            }
            _ => e.into(),
        })?;
        prepared.commit()?;

        // In-flight sequences can be dropped now, but they're about to go out of scope anyway.

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
                    Ok(vec![Event::from_slice(&e.expect("event exists"))])
                }
                Value::CLUSTER(ids) => {
                    let iter = ids.iter().map(|id| self.persy.read("events", &id));
                    itertools::process_results(iter, |iter| {
                        iter.map(|e| Event::from_slice(&e.expect("event exists")))
                            .collect()
                    })
                }
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn after(&self, sequence: u64, limit: Option<usize>) -> PRes<Vec<Event>> {
        // We want events _after_ this sequence.
        let sequence = sequence + 1;
        let limit = limit.unwrap_or(DEFAULT_LIMIT);

        // While this guard is held, sequences can't start being removed, unless they're already in
        // the process of being removed, which is okay because they must already be finished.
        let _guard = self.sequences.start_reading();

        let iter: IndexIter<u64, PersyId> = self.persy.range("sequence", sequence..)?;
        let iter = iter.take(limit).map(|(sequence, value)| match value {
            Value::SINGLE(id) => self.persy.read("events", &id).map(|e| (sequence, e)),
            Value::CLUSTER(_) => unreachable!("only one event per sequence"),
        });

        let events: PRes<Vec<_>> = itertools::process_results(iter, |iter| {
            iter.map(|(sequence, e)| {
                Event::from_slice_and_sequence(&e.expect("event exists"), Some(sequence))
            })
            .collect()
        });

        events.map(|mut events| {
            if let Some(min_in_flight_sequence) = self.sequences.min_in_flight_sequence() {
                // If we have any in-flight sequences, we only want to retain events where the sequence
                // is earlier than the min in-flight sequence.
                events.retain(|e| {
                    e.sequence
                        .expect("we've just constructed this Event with a sequence")
                        < min_in_flight_sequence
                })
            }

            events
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
    fn sink_multiple_events_to_aggregate() {
        let aggregate_id = Uuid::new_v4();
        let event_1 = NewEvent {
            aggregate_sequence: 1,
            event_type: String::new(),
            body: json!({}),
        };
        let event_2 = NewEvent {
            aggregate_sequence: 2,
            event_type: String::new(),
            body: json!({}),
        };

        let event_store = EventStore::new_temporary().unwrap();
        event_store
            .sink(vec![event_1, event_2], aggregate_id.clone())
            .unwrap();

        let events = event_store.for_aggregate(aggregate_id).unwrap();
        assert_eq!(events.len(), 2);
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
        assert_eq!(
            format!("{}", sink_2_result.unwrap_err()),
            format!("Stale aggregate: {}:{}", aggregate_id, 1)
        );
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
        // TODO: Handle this internally
        assert!(sink_2_result.is_err());
    }
}

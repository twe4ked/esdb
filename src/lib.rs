use byteorder::BigEndian;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use thiserror::Error;
use uuid::Uuid;
use zerocopy::{byteorder::U64, AsBytes, FromBytes, LayoutVerified, Unaligned};

use std::convert::TryInto;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

mod storage;

use storage::Storage;

const DEFAULT_LIMIT: usize = 1000;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct NewEvent {
    pub aggregate_sequence: u64,
    pub event_type: String,
    pub body: JsonValue,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Event {
    pub sequence: u64,
    pub aggregate_sequence: u64,
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub body: JsonValue,
}

impl Event {
    fn from_event_data(event_data: EventValue, sequence: u64) -> Self {
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
    storage: Storage,
    uuid_generator: Arc<dyn UuidGenerator>,
}

#[derive(Error, Debug, PartialEq)]
pub enum SinkError {
    #[error("Stale aggregate")]
    StaleAggregate,
    #[error("Event ID conflict")]
    EventIdConflict,
}

#[derive(FromBytes, AsBytes, Unaligned)]
#[repr(C)]
struct Sequence(U64<BigEndian>);

impl Sequence {
    fn new(value: u64) -> Self {
        Self(U64::new(value))
    }

    fn from_slice(bytes: &mut [u8]) -> u64 {
        let layout: LayoutVerified<&mut [u8], Self> =
            LayoutVerified::new_unaligned(&mut *bytes).expect("bytes do not fit schema");
        layout.into_ref().0.get()
    }
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

impl EventStore {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut storage = Storage::create_db(path).unwrap();

        Ok(Self::new_with_storage(storage))
    }

    pub fn new_with_storage(storage: Storage) -> Self {
        Self {
            storage,
            uuid_generator: Arc::new(UuidGeneratorV4),
        }
    }

    pub fn sink(&self, new_events: Vec<NewEvent>, aggregate_id: Uuid) -> Result<(), SinkError> {
        let event_id = self.uuid_generator.generate();

        let mut events = Vec::new();
        for new_event in new_events {
            let event = EventValue::from_new_event(new_event, aggregate_id, event_id);
            let blob = serde_json::to_vec(&event).unwrap();
            events.push(blob);
        }

        self.storage.append(events);

        Ok(())
    }

    pub fn for_aggregate(&self, aggregate_id: Uuid) -> io::Result<Vec<Event>> {
        // TODO: Actually find events for the aggregate
        self.after(0, None)
    }

    pub fn after(&self, sequence: u64, limit: Option<usize>) -> io::Result<Vec<Event>> {
        // TODO: Support sequence/limit
        // We want events _after_ this sequence.
        let _sequence = sequence + 1;
        let _limit = limit.unwrap_or(DEFAULT_LIMIT);

        // itertools::process_results(aggregates.scan_prefix(&aggregate_id.as_bytes()), |iter| {

        Ok(self
            .storage
            .events()?
            .iter()
            .map(|blob| {
                let event_data: EventValue = serde_json::from_slice(&blob).expect("decode error");
                let sequence = 1; // TODO
                Event::from_event_data(event_data, sequence)
            })
            .collect())
    }
}

fn to_u64(input: &[u8]) -> u64 {
    let input: [u8; 8] = (*input).try_into().unwrap();
    u64::from_be_bytes(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn basic() {
        let temp = tempdir();
        let mut path = PathBuf::from(temp.path());
        path.push("store.db");

        let storage = Storage::create_db(path).unwrap();

        let event_store = EventStore::new_with_storage(storage);

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
            assert_eq!(event.sequence, 1);
            assert_eq!(event.aggregate_sequence, 1);
            assert_eq!(event.aggregate_id, aggregate_id);
            assert_eq!(event.event_type, "foo_bar".to_string());
            assert_eq!(event.body, json!({"foo": "bar"}));
        }

        {
            let events = event_store.for_aggregate(aggregate_id).unwrap();
            let event = events.first().unwrap();
            assert_eq!(event.sequence, 1);
            assert_eq!(event.aggregate_sequence, 1);
            assert_eq!(event.aggregate_id, aggregate_id);
            assert_eq!(event.event_type, "foo_bar".to_string());
            assert_eq!(event.body, json!({"foo": "bar"}));
        }
    }

    // #[test]
    // fn sink_stale_aggregate() {
    //     use sled::transaction::TransactionError;
    //
    //     let event_store = EventStore::new().unwrap();
    //
    //     let aggregate_id = Uuid::new_v4();
    //     let event = NewEvent {
    //         aggregate_sequence: 1,
    //         event_type: String::new(),
    //         body: json!({}),
    //     };
    //
    //     let sink_1_result = event_store.sink(vec![event.clone()], aggregate_id.clone());
    //     assert!(sink_1_result.is_ok());
    //
    //     // The second sink will fail because the aggregate_sequence has already been used by this
    //     // aggregate.
    //     let sink_2_result = event_store.sink(vec![event], aggregate_id.clone());
    //     assert_eq!(
    //         sink_2_result.unwrap_err(),
    //         TransactionError::Abort(SinkError::StaleAggregate)
    //     );
    // }

    // #[test]
    // fn sink_duplicate_event_id() {
    //     use sled::transaction::TransactionError;
    //
    //     let mut event_store = EventStore::new().unwrap();
    //
    //     // Override the event UUID generator so we can make it generate a duplicate
    //     struct FakeUuidGenerator;
    //     impl UuidGenerator for FakeUuidGenerator {
    //         fn generate(&self) -> Uuid {
    //             Uuid::parse_str("0436430c-2b02-624c-2032-570501212b57").unwrap()
    //         }
    //     }
    //     event_store.uuid_generator = Arc::new(FakeUuidGenerator);
    //
    //     let event = NewEvent {
    //         aggregate_sequence: 1,
    //         event_type: String::new(),
    //         body: json!({}),
    //     };
    //
    //     let sink_1_result = event_store.sink(vec![event.clone()], Uuid::new_v4());
    //     assert!(sink_1_result.is_ok());
    //
    //     let sink_2_result = event_store.sink(vec![event], Uuid::new_v4());
    //     assert_eq!(
    //         sink_2_result.unwrap_err(),
    //         TransactionError::Abort(SinkError::EventIdConflict)
    //     );
    // }

    fn tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("esdb.")
            .rand_bytes(8)
            .tempdir()
            .expect("unable to create tempdir")
    }
}

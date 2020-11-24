use byteorder::BigEndian;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use sled::transaction::{abort, TransactionResult, Transactional};
use sled::{Config, Db};
use thiserror::Error;
use uuid::Uuid;
use zerocopy::{byteorder::U64, AsBytes, FromBytes, LayoutVerified, Unaligned};

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
    pub sequence: u64,
    pub aggregate_sequence: u64,
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub body: JsonValue,
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
            event_type,
            body,
        } = new_event;

        Event {
            sequence,
            aggregate_sequence,
            event_id,
            aggregate_id,
            event_type,
            created_at: Utc::now(),
            body,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
struct EventId(Uuid);

#[derive(Clone)]
pub struct EventStore {
    db: Db,
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
    pub fn new() -> sled::Result<Self> {
        let db = Config::default().temporary(true).open()?;
        Ok(Self::new_with_db(db))
    }

    pub fn new_with_db(db: Db) -> Self {
        Self {
            db,
            uuid_generator: Arc::new(UuidGeneratorV4),
        }
    }

    pub fn sink(
        &self,
        new_events: Vec<NewEvent>,
        aggregate_id: Uuid,
    ) -> TransactionResult<(), SinkError> {
        let aggregates = self.db.open_tree("aggregates")?;
        let events = self.db.open_tree("events")?;
        let seq = self.db.open_tree("seq")?;
        let event_ids = self.db.open_tree("event_ids")?;

        (&aggregates, &events, &seq, &event_ids).transaction(
            |(aggregates, events, seq, event_ids)| {
                for new_event in new_events.clone() {
                    let sequence: u64 = if let Some(mut current_sequence) = seq.get(b"seq")? {
                        let current_sequence = Sequence::from_slice(&mut current_sequence);
                        let new_sequence = current_sequence + 1;
                        seq.insert(b"seq", Sequence::new(new_sequence).as_bytes())?;
                        new_sequence
                    } else {
                        seq.insert(b"seq", Sequence::new(0).as_bytes())?;
                        0
                    };

                    // KEY: aggregate_id + aggregate_sequence
                    let mut aggregates_key = aggregate_id.as_bytes().to_vec();
                    aggregates_key.extend_from_slice(&new_event.aggregate_sequence.to_be_bytes());

                    // The aggregate_id + aggregate_sequence here needs to be unique so we can be sure
                    // we don't have a stale aggregate.
                    if aggregates.get(&aggregates_key)?.is_some() {
                        // We can't handle the stale aggregate error as the caller will need to reload the
                        // aggregate and try again.
                        return abort(SinkError::StaleAggregate)?;
                    }
                    aggregates.insert(aggregates_key, serde_json::to_vec(&sequence).unwrap())?;

                    // Generate an ID and use it as a key, this way the transaction will try again
                    // if it's not unique.
                    let event_id = self.uuid_generator.generate();
                    let event_ids_key = event_id.as_bytes().to_vec();
                    if event_ids.get(&event_ids_key)?.is_some() {
                        return abort(SinkError::EventIdConflict)?;
                    } else {
                        event_ids.insert(event_ids_key, &[])?;
                    }

                    let event = Event::from_new_event(new_event, aggregate_id, sequence, event_id);

                    // KEY: sequence
                    events.insert(&sequence.to_be_bytes(), serde_json::to_vec(&event).unwrap())?;
                }

                Ok(())
            },
        )?;

        // Flush the database after each sink.
        self.db.flush()?;

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

        // Fetch the events and convert them info `Event`s.
        Ok(self
            .db
            .open_tree("events")?
            .range(sequence.to_be_bytes()..)
            .map(|e| e.unwrap())
            .map(|(_, e)| -> Event { serde_json::from_slice(&e).expect("decode error") })
            .take(limit)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sink_stale_aggregate() {
        use sled::transaction::TransactionError;

        let event_store = EventStore::new().unwrap();

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
            sink_2_result.unwrap_err(),
            TransactionError::Abort(SinkError::StaleAggregate)
        );
    }

    #[test]
    fn sink_duplicate_event_id() {
        use sled::transaction::TransactionError;

        let mut event_store = EventStore::new().unwrap();

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
        assert_eq!(
            sink_2_result.unwrap_err(),
            TransactionError::Abort(SinkError::EventIdConflict)
        );
    }
}

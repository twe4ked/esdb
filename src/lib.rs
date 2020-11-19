use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use sled::transaction::{abort, TransactionResult, Transactional};
use sled::{Config, Db};
use thiserror::Error;
use uuid::Uuid;

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
}

#[derive(Error, Debug, PartialEq)]
pub enum SinkError {
    #[error("Stale aggregate")]
    StaleAggregate,
}

impl EventStore {
    pub fn new() -> sled::Result<Self> {
        let db = Config::default().temporary(true).open()?;
        Ok(Self::new_with_db(db))
    }

    pub fn new_with_db(db: Db) -> Self {
        Self { db }
    }

    pub fn sink(
        &self,
        new_events: Vec<NewEvent>,
        aggregate_id: Uuid,
    ) -> TransactionResult<(), SinkError> {
        let aggregates = self.db.open_tree("aggregates")?;
        let events = self.db.open_tree("events")?;
        let seq = self.db.open_tree("seq")?;

        (&aggregates, &events, &seq).transaction(|(aggregates, events, seq)| {
            for new_event in new_events.clone() {
                let sequence: u64 = if let Some(current_sequence) = seq.get(b"seq")? {
                    let current_sequence: u64 =
                        serde_json::from_slice(&current_sequence).expect("decode error");
                    let new_sequence = current_sequence + 1;
                    seq.insert(b"seq", serde_json::to_vec(&new_sequence).unwrap())?;
                    new_sequence
                } else {
                    seq.insert(b"seq", serde_json::to_vec(&0).unwrap())?;
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

                let event_id = Uuid::new_v4(); // TODO: Ensure uniqueness
                let event = Event::from_new_event(new_event, aggregate_id, sequence, event_id);

                // KEY: sequence
                events.insert(&sequence.to_be_bytes(), serde_json::to_vec(&event).unwrap())?;
            }

            Ok(())
        })?;

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
            aggregate_type: String::new(),
            event_type: String::new(),
            body: json!({}),
            metadata: json!({}),
        };

        let sink_1_result = event_store.sink(vec![event.clone()], aggregate_id.clone());
        assert!(sink_1_result.is_ok());

        // The second sink will fail because the aggregate_sequence has already been used by this
        // aggregate.
        let sink_2_result = event_store.sink(vec![event.clone()], aggregate_id.clone());
        assert_eq!(
            sink_2_result.unwrap_err(),
            TransactionError::Abort(SinkError::StaleAggregate)
        );
    }
}

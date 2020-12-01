use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use uuid::Uuid;

mod event_store;
mod sequences;

pub use crate::event_store::EventStore;
use crate::event_store::EventValue;

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

use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::Value as JsonValue;
use uuid::Uuid;

mod event_store;
mod sequences;

pub use crate::event_store::EventStore;

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

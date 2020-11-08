use chrono::prelude::*;
use serde_json::json;
use sled::Config;
use uuid::Uuid;

use esdb::{Event, EventStore};

fn main() {
    let db = Config::default().temporary(true).open().unwrap();
    let event_store = EventStore::new_with_db(db);

    let aggregate_id = Uuid::new_v4();

    let event_1 = Event {
        sequence: 1,
        aggregate_sequence: 1,
        event_id: Uuid::new_v4(),
        aggregate_id,
        aggregate_type: "Foo".to_string(),
        event_type: "Fooed".to_string(),
        body: json!({ "an": "object" }),
        metadata: json!({ "an": "object" }),
        created_at: Utc::now(),
    };
    let event_2 = Event {
        sequence: 2,
        aggregate_sequence: 2,
        event_id: Uuid::new_v4(),
        aggregate_id,
        aggregate_type: "Foo".to_string(),
        event_type: "Fooed".to_string(),
        body: json!({ "an": "object" }),
        metadata: json!({ "an": "object" }),
        created_at: Utc::now(),
    };
    let event_3 = Event {
        sequence: 3,
        aggregate_sequence: 1,
        event_id: Uuid::new_v4(),
        aggregate_id: Uuid::new_v4(), // Different aggregate
        aggregate_type: "Foo".to_string(),
        event_type: "Fooed".to_string(),
        body: json!({ "an": "object" }),
        metadata: json!({ "an": "object" }),
        created_at: Utc::now(),
    };

    event_store.sink(event_1).unwrap();
    event_store.sink(event_2).unwrap();
    event_store.sink(event_3).unwrap();

    dbg!(event_store.for_aggregate(aggregate_id));
    eprintln!("--------------------------------------------------------------------------------");
    dbg!(event_store.after(1));
}

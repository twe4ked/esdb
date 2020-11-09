use serde_json::json;
use sled::Config;
use uuid::Uuid;

use esdb::{EventStore, NewEvent};

fn main() {
    let db = Config::default().temporary(true).open().unwrap();
    let event_store = EventStore::new_with_db(db);

    let event_1 = NewEvent {
        aggregate_sequence: 1,
        aggregate_type: "Foo".to_string(),
        event_type: "Fooed".to_string(),
        body: json!({ "an": "object" }),
        metadata: json!({ "an": "object" }),
    };
    let event_2 = NewEvent {
        aggregate_sequence: 2,
        aggregate_type: "Foo".to_string(),
        event_type: "Fooed".to_string(),
        body: json!({ "an": "object" }),
        metadata: json!({ "an": "object" }),
    };
    let event_3 = NewEvent {
        aggregate_sequence: 1,
        aggregate_type: "Foo".to_string(),
        event_type: "Fooed".to_string(),
        body: json!({ "an": "object" }),
        metadata: json!({ "an": "object" }),
    };

    let aggregate_id_1 = Uuid::new_v4();
    let aggregate_id_2 = Uuid::new_v4();

    event_store.sink(event_1, aggregate_id_1).unwrap();
    event_store.sink(event_2, aggregate_id_1).unwrap();
    event_store.sink(event_3, aggregate_id_2).unwrap();

    dbg!(event_store.for_aggregate(aggregate_id_1));
    eprintln!("--------------------------------------------------------------------------------");
    dbg!(event_store.after(1));
}

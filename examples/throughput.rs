use esdb::{EventStore, NewEvent};
use serde_json::json;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use uuid::Uuid;

const EVENTS_PER_THREAD: usize = 100;
const THREADS_COUNT: usize = 10;
const TOTAL_EVENTS: usize = EVENTS_PER_THREAD * THREADS_COUNT;

fn main() {
    let event_store = Arc::new(EventStore::new_temporary().unwrap());

    let now = Instant::now();

    let mut handles = Vec::new();
    for n in 0..THREADS_COUNT {
        handles.push(spawn_thread(n, event_store.clone()));
    }

    println!("Waiting...");
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = now.elapsed();

    println!();
    println!("Total events\t{}", TOTAL_EVENTS);
    println!("Total time\t{}ms", elapsed.as_millis());
}

fn spawn_thread(n: usize, event_store: Arc<EventStore>) -> std::thread::JoinHandle<()> {
    thread::spawn(move || {
        for _ in 0..EVENTS_PER_THREAD {
            let aggregate_id = Uuid::new_v4();
            let event = NewEvent {
                aggregate_sequence: 1,
                event_type: "Foo".to_string(),
                body: json!({}),
            };
            event_store.sink(vec![event], aggregate_id).unwrap();
        }

        println!("Thread finishing {}", n);
    })
}

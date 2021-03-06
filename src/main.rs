use serde::{Deserialize, Serialize};
use sled::Config;
use tracing_subscriber::fmt::format::FmtSpan;
use uuid::Uuid;
use warp::http::StatusCode;
use warp::Filter;

use esdb::{EventStore, NewEvent};

// POST /sink/:aggregate_id
fn sink(
    event_store: EventStore,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::post()
        .and(warp::path("sink"))
        .and(warp::path::param::<Uuid>())
        // Only accept bodies smaller than 16kb...
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .map(move |aggregate_id, events: Vec<NewEvent>| {
            event_store.sink(events, aggregate_id).unwrap();
            Ok(StatusCode::NO_CONTENT)
        })
}

#[derive(Deserialize, Serialize)]
struct AfterQuery {
    limit: Option<usize>,
}

// GET /after/:sequence?limit=100
fn after(
    event_store: EventStore,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path("after"))
        .and(warp::path::param::<u64>())
        .and(warp::query::<AfterQuery>())
        .map(move |sequence, query: AfterQuery| {
            let events = event_store.after(sequence, query.limit).unwrap();
            warp::reply::json(&events)
        })
}

// GET /aggregate/:aggregate_id
fn aggregate(
    event_store: EventStore,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path("aggregate"))
        .and(warp::path::param::<Uuid>())
        .map(move |aggregate_id| {
            let events = event_store.for_aggregate(aggregate_id).unwrap();
            warp::reply::json(&events)
        })
}

pub fn routes(
    event_store: EventStore,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    sink(event_store.clone())
        .or(after(event_store.clone()))
        .or(aggregate(event_store))
}

#[tokio::main]
async fn main() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "tracing=info,warp=debug".to_owned());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        // Record an event when each span closes. This can be used to time our route durations
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let db = if let Ok(path) = std::env::var("DATABASE_PATH") {
        Config::default().path(path).open()
    } else {
        Config::default().temporary(true).open()
    }
    .unwrap();
    let event_store = EventStore::new_with_db(db);

    warp::serve(routes(event_store).with(warp::trace::request()))
        .run(([127, 0, 0, 1], 3030))
        .await
}

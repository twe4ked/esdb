use sled::Db;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone)]
pub struct Sequences {
    in_flight_sequences: Arc<RwLock<HashSet<u64>>>,
    finished_sequences: Arc<RwLock<HashSet<u64>>>,
    readers_count: Arc<AtomicU64>,
    generate_sequence_lock: Arc<Mutex<()>>,
}

impl Sequences {
    pub fn new() -> Self {
        Self {
            // List of in-flight sequences
            in_flight_sequences: Arc::new(RwLock::new(HashSet::new())),
            // List of sequences that have been marked as "finished" ready for removal
            finished_sequences: Arc::new(RwLock::new(HashSet::new())),
            // Number of threads currently reading. This is used to ensure we don't remove
            // in-flight sequences while threads might be using them.
            readers_count: Arc::new(AtomicU64::new(0)),
            // Used to ensure newly generated sequences is marked as in-flight before they're used.
            generate_sequence_lock: Arc::new(Mutex::new(())),
        }
    }

    /// When generating a sequence we need to do it in a lock so that we can be sure that no other
    /// IDs get generated before we mark the new sequence as in-flight.
    pub fn generate(&self, db: &Db) -> sled::Result<NewSequence> {
        match self.generate_sequence_lock.lock() {
            Ok(_) => {
                let sequence = db.generate_id()?;
                // Start sequence at 1.
                let sequence = sequence + 1;
                self.mark_sequence_in_flight(sequence);
                Ok(NewSequence::new(sequence, &self))
            }
            Err(e) => panic!("{}", e),
        }
    }

    pub fn min_in_flight_sequence(&self) -> Option<u64> {
        self.in_flight_sequences
            .read()
            .unwrap()
            .iter()
            .min()
            .copied()
    }

    pub fn start_reading(&self) -> SequenceGuard {
        // Lock removing of in-flight sequences while we fetch events
        self.readers_count.fetch_add(1, Ordering::SeqCst);
        SequenceGuard::new(&self)
    }

    fn mark_sequence_in_flight(&self, sequence: u64) {
        self.in_flight_sequences.write().unwrap().insert(sequence);
    }

    fn mark_sequence_finished(&self, sequence: u64) {
        self.finished_sequences.write().unwrap().insert(sequence);
    }

    fn finished_reading(&self) {
        self.readers_count.fetch_sub(1, Ordering::SeqCst);

        // If there are no other readers we're safe to remove in-flight sequences
        if self.readers_count.load(Ordering::SeqCst) == 0 {
            // Read the finished sequences first
            let mut finished_sequences = self.finished_sequences.write().unwrap();

            // Remove any sequences that are no longer in flight
            let mut in_flight_sequences = self.in_flight_sequences.write().unwrap();
            for sequence in finished_sequences.drain() {
                in_flight_sequences.remove(&sequence);
            }
        }
    }
}

pub struct NewSequence<'a> {
    pub value: u64,
    sequences: &'a Sequences,
}

impl<'a> NewSequence<'a> {
    pub fn new(value: u64, sequences: &'a Sequences) -> Self {
        Self { value, sequences }
    }
}

impl Drop for NewSequence<'_> {
    fn drop(&mut self) {
        self.sequences.mark_sequence_finished(self.value);
    }
}

#[must_use]
pub struct SequenceGuard<'a> {
    sequences: &'a Sequences,
}

impl<'a> SequenceGuard<'a> {
    fn new(sequences: &'a Sequences) -> Self {
        Self { sequences }
    }
}

impl Drop for SequenceGuard<'_> {
    fn drop(&mut self) {
        self.sequences.finished_reading();
    }
}

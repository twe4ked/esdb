use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone)]
pub struct Sequences {
    min_in_flight_sequence: Arc<AtomicU64>,
    readers_count: Arc<AtomicU64>,
    generate_sequence_lock: Arc<Mutex<()>>,
    sequence: Arc<AtomicU64>,
}

impl Sequences {
    pub fn new() -> Self {
        Self {
            // Min in-flight sequence
            min_in_flight_sequence: Arc::new(AtomicU64::new(0)),
            //  Max finished sequence
            finished_sequence: Arc::new(AtomicU64::new(0)),
            // Number of threads currently reading. This is used to ensure we don't remove
            // in-flight sequences while threads might be using them.
            readers_count: Arc::new(AtomicU64::new(0)),
            // Used to ensure newly generated sequences is marked as in-flight before they're used.
            generate_sequence_lock: Arc::new(Mutex::new(())),
            // Start sequence at 1.
            // TODO: Find current sequence
            sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    /// When generating a sequence we need to do it in a lock so that we can be sure that no other
    /// IDs get generated before we mark the new sequence as in-flight.
    pub fn generate(&self) -> NewSequence {
        match self.generate_sequence_lock.lock() {
            Ok(_) => {
                let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
                self.mark_sequence_in_flight(sequence);
                NewSequence::new(sequence, &self)
            }
            Err(e) => panic!("{}", e),
        }
    }

    pub fn min_in_flight_sequence(&self) -> Option<u64> {
        let x = self.min_in_flight_sequence.load(Ordering::SeqCst);
        if x == 0 {
            None
        } else {
            Some(x)
        }
    }

    pub fn start_reading(&self) -> SequenceGuard {
        // Lock removing of in-flight sequences while we fetch events
        self.readers_count.fetch_add(1, Ordering::SeqCst);
        SequenceGuard::new(&self)
    }

    fn mark_sequence_in_flight(&self, sequence: u64) {
        self.min_in_flight_sequence.set(sequence);
    }

    fn mark_sequence_finished(&self, sequence: u64) {
        self.finished_sequence.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |current_max_sequence_finished| {
                if sequence > current_max_sequence_finished {
                    Some(sequence)
                } else {
                    None
                }
            },
        );

        self.try_remove_finished_in_flight_sequences();
    }

    fn finished_reading(&self) {
        self.readers_count.fetch_sub(1, Ordering::SeqCst);
        self.try_remove_finished_in_flight_sequences();
    }

    fn try_remove_finished_in_flight_sequences(&self) {
        // If there are no other readers we're safe to remove in-flight sequences
        if self.readers_count.load(Ordering::SeqCst) == 0 {
            // Read the finished sequences first
            let mut finished_sequences = self.finished_sequence.fetch(Ordering::SeqCst);

            // Remove any sequences that are no longer in flight
            // self.min_in_flight_sequence.fetch_update(
            //     Ordering::SeqCst,
            //     Ordering::SeqCst,
            //     |current_min_in_flight_sequence| {
            //         if current_min_in_flight_sequence == sequence {
            //             // There are no in flight sequences
            //             Some(0)
            //         } else if current_min_in_flight_sequence > sequence
            //
            //     },
            // );
        }
    }
}

#[must_use]
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

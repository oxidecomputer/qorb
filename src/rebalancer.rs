//! Utilities to help rebalance the priority list for connections

use crate::backend;
use crate::priority_list::WeightedValue;

use rand::{rng, Rng};

// The "scores" assigned to backends are "punitive", in the sense
// that adding to the score makes a backend less preferable.

const BACKEND_START_SCORE: usize = 20;
const BACKEND_CLAIM_SCORE: usize = 1;
const BACKEND_CLAIM_FAIL_SCORE: usize = 20;
const BACKEND_JITTER_SCORE: usize = 5;

pub fn new_backend(name: backend::Name) -> WeightedValue<backend::Name> {
    WeightedValue {
        score: BACKEND_START_SCORE,
        value: name,
    }
}

pub fn claimed_ok(backend: &mut WeightedValue<backend::Name>) {
    backend.score += BACKEND_CLAIM_SCORE;
}

pub fn claimed_err(backend: &mut WeightedValue<backend::Name>) {
    backend.score += BACKEND_CLAIM_FAIL_SCORE;
}

pub fn add_random_jitter(backend: &mut WeightedValue<backend::Name>) {
    let mut rng = rng();
    backend.score += rng.random_range(0..BACKEND_JITTER_SCORE);
}

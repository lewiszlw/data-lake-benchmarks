// SPDX-License-Identifier: MIT

//! Engine-independent pieces of the benchmark.
//!
//! Arrow types intentionally do not appear here: IndexLake and Lance use
//! different Arrow major versions, so each adapter builds its own record
//! batches from the primitives in [`data`].

pub mod cli;
pub mod config;
pub mod data;
pub mod stats;

pub use cli::{emit, parse_cli, Cli};
pub use config::BenchConfig;
pub use data::{
    byte_at, dataset_vector, f32_at, ground_truth, query_vector, scalar_blob, scalar_payload,
    scalar_raw_bytes, splitmix64, vector_raw_bytes, DATA_SEED, QUERY_SEED, SCALAR_BLOB_LEN,
    SCALAR_PAYLOAD_LEN,
};
pub use stats::{mib, mib_per_second, rows_per_second, stats, Stats};

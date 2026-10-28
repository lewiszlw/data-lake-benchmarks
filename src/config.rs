// SPDX-License-Identifier: MIT

//! Workload, object-store, and catalog configuration.

use std::env;

fn env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u16(key: &str, default: u16) -> u16 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_i32(key: &str, default: i32) -> i32 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    // Workload shape.
    pub scalar_rows: usize,
    pub scalar_batch: usize,
    pub inline_limit: usize,
    pub row_group: usize,
    pub vector_rows: usize,
    pub vector_batch: usize,
    pub dim: usize,

    // Query shape.
    pub k: usize,
    pub ef_search: usize,
    pub ef_construction: usize,
    pub range_queries: usize,
    pub range_rows: usize,
    pub ann_queries: usize,
    pub recall_queries: usize,
    pub concurrency: usize,

    // Conditional mutation shape.
    pub mutation_bucket: i32,
    pub mutation_windows: usize,

    // Single-row insert shape.
    pub single_inserts: usize,

    // Object store.
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_region: String,
    pub lance_prefix: String,

    // IndexLake catalog.
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
}

impl BenchConfig {
    pub fn from_env() -> Self {
        Self {
            scalar_rows: env_usize("PERF_SCALAR_ROWS", 1_000_000),
            scalar_batch: env_usize("PERF_SCALAR_BATCH", 10_000),
            inline_limit: env_usize("PERF_INLINE_LIMIT", 100_000),
            row_group: env_usize("PERF_ROW_GROUP", 10_000),
            vector_rows: env_usize("PERF_VECTOR_ROWS", 100_000),
            vector_batch: env_usize("PERF_VECTOR_BATCH", 1_000),
            dim: env_usize("PERF_DIM", 1024),

            k: env_usize("PERF_K", 10),
            ef_search: env_usize("PERF_EF_SEARCH", 256),
            ef_construction: env_usize("PERF_EF_CONSTRUCTION", 400),
            range_queries: env_usize("PERF_RANGE_QUERIES", 200),
            range_rows: env_usize("PERF_RANGE_ROWS", 10_000),
            ann_queries: env_usize("PERF_ANN_QUERIES", 100),
            recall_queries: env_usize("PERF_RECALL_QUERIES", 20),
            concurrency: env_usize("PERF_CONCURRENCY", 8),

            mutation_bucket: env_i32("PERF_MUTATION_BUCKET", 7),
            mutation_windows: env_usize("PERF_MUTATION_WINDOWS", 3),

            single_inserts: env_usize("PERF_SINGLE_INSERTS", 100),

            s3_endpoint: env_string("S3_ENDPOINT", "http://127.0.0.1:9000"),
            s3_bucket: env_string("S3_BUCKET", "indexlake"),
            s3_access_key: env_string("S3_ACCESS_KEY", "admin"),
            s3_secret_key: env_string("S3_SECRET_KEY", "password"),
            s3_region: env_string("S3_REGION", "us-east-1"),
            lance_prefix: env_string("PERF_LANCE_PREFIX", "perf-eval/lance"),

            pg_host: env_string("PGHOST", "localhost"),
            pg_port: env_u16("PGPORT", 5432),
            pg_user: env_string("PGUSER", "postgres"),
            pg_password: env_string("PGPASSWORD", "password"),
            pg_database: env_string("PGDATABASE", "postgres"),
        }
    }

    pub fn namespace(&self, run_id: &str) -> String {
        format!("perf_{run_id}")
    }

    pub fn lance_dataset_uri(&self, run_id: &str, table: &str) -> String {
        format!(
            "s3://{}/{}/{}/{}",
            self.s3_bucket, self.lance_prefix, run_id, table
        )
    }

    pub fn expected_files(&self, rows: usize) -> usize {
        rows.div_ceil(self.inline_limit)
    }
}

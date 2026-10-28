// SPDX-License-Identifier: MIT

//! Deterministic workload generation and exact ground truth.
//!
//! These values are engine-independent and Arrow-free so both adapters ingest
//! byte-identical data regardless of their Arrow major version.

pub const DATA_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
pub const QUERY_SEED: u64 = 0xd1b5_4a32_d192_ed03;
pub const SCALAR_PAYLOAD_LEN: usize = 128;
pub const SCALAR_BLOB_LEN: usize = 128;

/// Number of distinct buckets used by the conditional mutation workload.
pub const MUTATION_BUCKETS: usize = 100;
/// Distance between buckets of consecutive mutation windows.
pub const MUTATION_BUCKET_STRIDE: usize = 10;

/// SplitMix64. Kept deterministic so every engine produces the same inputs.
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn byte_at(seed: u64, row: usize, index: usize) -> u8 {
    let x = splitmix64(seed ^ (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (index as u64));
    (x >> 24) as u8
}

pub fn f32_at(seed: u64, row: usize, dim: usize) -> f32 {
    let x = splitmix64(
        seed ^ (row as u64).wrapping_mul(0xd6e8_feb8_6659_fd93)
            ^ (dim as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
    );
    let unit = (x >> 40) as f32 / (1u32 << 24) as f32;
    unit * 2.0 - 1.0
}

pub fn dataset_vector(row: usize, dim: usize) -> Vec<f32> {
    (0..dim).map(|d| f32_at(DATA_SEED, row, d)).collect()
}

pub fn query_vector(query: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|d| f32_at(QUERY_SEED.wrapping_add(query as u64), query, d))
        .collect()
}

/// Deterministic start id of a contiguous range scan.
///
/// Starts are spread across the whole id domain so a range usually crosses a
/// row-group boundary instead of always landing on the same aligned block.
pub fn range_start(rows: usize, range_rows: usize, sample: usize) -> usize {
    assert!(
        range_rows > 0 && range_rows <= rows,
        "range_rows must be in 1..=rows"
    );
    let span = rows - range_rows + 1;
    (splitmix64(QUERY_SEED ^ sample as u64) % span as u64) as usize
}

/// A compressible text value of exactly [`SCALAR_PAYLOAD_LEN`] bytes.
///
/// The row prefix keeps values unique; the repeated sentence is what
/// compression algorithms can exploit, mimicking a real text column.
const PAYLOAD_TEMPLATE: &str = "the quick brown fox jumps over the lazy dog. pack my box with five dozen liquor jugs. how vexingly quick daft zebras jump! ";

pub fn scalar_payload(row: usize) -> String {
    let mut value = format!("{row:08}-");
    let need = SCALAR_PAYLOAD_LEN.saturating_sub(value.len());
    let take = need.min(PAYLOAD_TEMPLATE.len());
    value.push_str(&PAYLOAD_TEMPLATE[..take]);
    while value.len() < SCALAR_PAYLOAD_LEN {
        value.push('x');
    }
    value
}

pub fn scalar_blob(row: usize) -> Vec<u8> {
    (0..SCALAR_BLOB_LEN)
        .map(|i| byte_at(DATA_SEED, row, i))
        .collect()
}

pub fn scalar_raw_bytes(rows: usize) -> u64 {
    (rows * (4 + SCALAR_PAYLOAD_LEN + SCALAR_BLOB_LEN)) as u64
}

/// Bucket of a row in the conditional mutation workload.
///
/// Every bucket holds roughly `1 / MUTATION_BUCKETS` of the table, and unlike
/// an `id` range no Parquet row-group statistic can prune it.
pub fn mutation_bucket(row: usize) -> i32 {
    (row % MUTATION_BUCKETS) as i32
}

/// Exact number of rows whose bucket equals `bucket`.
pub fn mutation_bucket_rows(rows: usize, bucket: i32) -> usize {
    assert!(bucket >= 0, "mutation bucket must be non-negative");
    let bucket = bucket as usize;
    if bucket >= MUTATION_BUCKETS {
        return 0;
    }
    rows / MUTATION_BUCKETS + usize::from(bucket < rows % MUTATION_BUCKETS)
}

/// The row offset of the `ordinal`-th row in `bucket`.
pub fn mutation_bucket_id(rows: usize, bucket: i32, ordinal: usize) -> i32 {
    assert!(ordinal < mutation_bucket_rows(rows, bucket));
    let row = ordinal * MUTATION_BUCKETS + bucket as usize;
    row as i32
}

/// Spread `count` deterministic sample ids across `bucket`, for verification.
pub fn mutation_sample_ids(rows: usize, bucket: i32, count: usize) -> Vec<i32> {
    let available = mutation_bucket_rows(rows, bucket);
    let take = count.min(available);
    (0..take)
        .map(|i| {
            let ordinal = if take == 1 {
                0
            } else {
                i * (available - 1) / (take - 1)
            };
            mutation_bucket_id(rows, bucket, ordinal)
        })
        .collect()
}

/// First row whose bucket is not `bucket`, used to verify non-target rows.
pub fn mutation_first_row_outside_bucket(rows: usize, bucket: i32) -> usize {
    (0..rows)
        .find(|row| mutation_bucket(*row) != bucket)
        .expect("table must contain at least one row outside the target bucket")
}

/// Buckets for the conditional mutation windows.
///
/// Windows are spaced by [`MUTATION_BUCKET_STRIDE`] so their row sets are
/// disjoint.
pub fn mutation_window_buckets(base: i32, windows: usize) -> Vec<i32> {
    (0..windows)
        .map(|window| {
            let bucket = base + (window * MUTATION_BUCKET_STRIDE) as i32;
            assert!(
                (0..MUTATION_BUCKETS as i32).contains(&bucket),
                "mutation bucket {bucket} is out of range; lower PERF_MUTATION_BUCKET or PERF_MUTATION_WINDOWS"
            );
            bucket
        })
        .collect()
}

const MUTATION_PAYLOAD_TEMPLATE: &str = "conditional mutation benchmark updated value. ";

/// Fixed-width replacement payload for conditional updates.
///
/// Keeping the width equal to [`SCALAR_PAYLOAD_LEN`] avoids changing the
/// logical row size, so mutation results stay comparable with ingest results.
pub fn mutation_payload(bucket: i32) -> String {
    let mut value = format!("updated-{bucket:02}-");
    let need = SCALAR_PAYLOAD_LEN.saturating_sub(value.len());
    let take = need.min(MUTATION_PAYLOAD_TEMPLATE.len());
    value.push_str(&MUTATION_PAYLOAD_TEMPLATE[..take]);
    while value.len() < SCALAR_PAYLOAD_LEN {
        value.push('m');
    }
    value
}

pub fn vector_raw_bytes(rows: usize, dim: usize) -> u64 {
    (rows * (4 + dim * 4)) as u64
}

/// Exact top-k by squared L2 over the generated vectors.
///
/// Returned ids are the dataset's user-visible `id` values, which equal the
/// generation row index.
pub fn ground_truth(queries: &[Vec<f32>], rows: usize, dim: usize, k: usize) -> Vec<Vec<i32>> {
    let vectors: Vec<Vec<f32>> = (0..rows).map(|row| dataset_vector(row, dim)).collect();
    queries
        .iter()
        .map(|query| {
            let mut best: Vec<(f32, i32)> = Vec::with_capacity(k + 1);
            for (row, vector) in vectors.iter().enumerate() {
                let distance: f32 = query
                    .iter()
                    .zip(vector)
                    .map(|(a, b)| {
                        let delta = *a - *b;
                        delta * delta
                    })
                    .sum();
                if best.len() < k {
                    best.push((distance, row as i32));
                    if best.len() == k {
                        best.sort_by(|a, b| a.0.total_cmp(&b.0));
                    }
                } else if distance < best[k - 1].0 {
                    best[k - 1] = (distance, row as i32);
                    best.sort_by(|a, b| a.0.total_cmp(&b.0));
                }
            }
            best.into_iter().map(|(_, id)| id).collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_fixed_width_and_unique() {
        let a = scalar_payload(0);
        let b = scalar_payload(42);
        assert_eq!(a.len(), SCALAR_PAYLOAD_LEN);
        assert_eq!(b.len(), SCALAR_PAYLOAD_LEN);
        assert_ne!(a, b);
    }

    #[test]
    fn vectors_are_deterministic_and_bounded() {
        let a = dataset_vector(7, 16);
        let b = dataset_vector(7, 16);
        assert_eq!(a, b);
        assert_eq!(a.len(), 16);
        assert!(a.iter().all(|value| (-1.0..=1.0).contains(value)));
        assert_ne!(query_vector(0, 16), query_vector(1, 16));
    }

    #[test]
    fn ground_truth_returns_k_distinct_ids() {
        let queries = vec![vec![0.0; 4], vec![0.5; 4]];
        let truth = ground_truth(&queries, 32, 4, 5);
        assert_eq!(truth.len(), 2);
        for ids in truth {
            assert_eq!(ids.len(), 5);
            let unique: std::collections::HashSet<_> = ids.iter().collect();
            assert_eq!(unique.len(), 5);
            assert!(ids.iter().all(|id| (0..32).contains(id)));
        }
    }

    #[test]
    fn mutation_bucket_counts_are_exact() {
        assert_eq!(mutation_bucket_rows(1_000_000, 7), 10_000);
        assert_eq!(mutation_bucket_rows(105, 4), 2);
        assert_eq!(mutation_bucket_rows(105, 5), 1);
        assert_eq!(mutation_bucket_rows(99, 99), 0);
        assert_eq!(mutation_bucket_rows(100, 99), 1);
        assert_eq!(mutation_bucket_rows(99, 100), 0);
        assert_eq!(
            (0..105).filter(|row| mutation_bucket(*row) == 4).count(),
            mutation_bucket_rows(105, 4)
        );
    }

    #[test]
    fn mutation_samples_stay_inside_the_bucket() {
        let ids = mutation_sample_ids(1_000_000, 7, 8);
        assert_eq!(ids.len(), 8);
        assert_eq!(ids.first(), Some(&7));
        assert!(ids.iter().all(|id| mutation_bucket(*id as usize) == 7));
        assert_eq!(mutation_sample_ids(1_000_000, 7, 100_000).len(), 10_000);
    }

    #[test]
    fn mutation_window_buckets_are_disjoint() {
        assert_eq!(mutation_window_buckets(7, 3), vec![7, 17, 27]);
        let unique: std::collections::HashSet<_> =
            mutation_window_buckets(0, 10).into_iter().collect();
        assert_eq!(unique.len(), 10);
    }

    #[test]
    fn range_starts_stay_in_bounds() {
        let rows = 1_000_000;
        let range_rows = 10_000;
        assert_eq!(range_start(rows, rows, 0), 0);
        let starts: Vec<usize> = (0..100)
            .map(|sample| range_start(rows, range_rows, sample))
            .collect();
        assert!(starts.iter().all(|start| start + range_rows <= rows));
        assert_eq!(range_start(rows, range_rows, 7), starts[7]);
        let unique: std::collections::HashSet<_> = starts.into_iter().collect();
        assert!(unique.len() > 90);
    }

    #[test]
    fn mutation_payload_is_fixed_width_and_distinct() {
        let payload = mutation_payload(7);
        assert_eq!(payload.len(), SCALAR_PAYLOAD_LEN);
        assert_ne!(payload, mutation_payload(17));
        assert_ne!(payload, scalar_payload(7));
    }
}

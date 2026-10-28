// SPDX-License-Identifier: MIT

//! Latency and throughput helpers.

use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub p50: f64,
    pub p95: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
}

/// Latency samples are in milliseconds.
pub fn stats(mut samples: Vec<f64>) -> Stats {
    assert!(!samples.is_empty(), "latency samples must not be empty");
    samples.sort_by(|a, b| a.total_cmp(b));
    let last = samples.len() - 1;
    let percentile = |q: f64| samples[((last as f64) * q).round() as usize];
    Stats {
        p50: percentile(0.50),
        p95: percentile(0.95),
        mean: samples.iter().sum::<f64>() / samples.len() as f64,
        min: samples[0],
        max: samples[last],
    }
}

pub fn rows_per_second(rows: usize, elapsed: Duration) -> f64 {
    rows as f64 / elapsed.as_secs_f64()
}

pub fn mib_per_second(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
}

pub fn mib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_edges_are_exact() {
        let stats = stats((1..=100).map(|value| value as f64).collect());
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 100.0);
        assert_eq!(stats.p50, 51.0);
        assert_eq!(stats.p95, 95.0);
        assert_eq!(stats.mean, 50.5);
    }

    #[test]
    fn throughput_helpers_use_seconds() {
        assert_eq!(rows_per_second(1000, Duration::from_secs(2)), 500.0);
        assert_eq!(mib_per_second(4 * 1024 * 1024, Duration::from_secs(1)), 4.0);
    }
}

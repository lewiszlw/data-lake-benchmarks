// SPDX-License-Identifier: MIT

//! DataFusion ListingTable baseline: full scan and range scan over Parquet.
//!
//! The Parquet dataset is written by this binary with the same deterministic
//! rows as the other adapters. Only query metrics are reported; the write phase
//! exists to prepare the listing table.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, BinaryBuilder, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use data_lake_benchmarks::{
    emit, mib_per_second, parse_cli, range_start, rows_per_second, scalar_blob, scalar_payload,
    scalar_raw_bytes, stats, BenchConfig, SCALAR_BLOB_LEN,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use url::Url;

const USAGE: &str = "<ingest|query|query-scalar> [run-id]";

fn s3_store(cfg: &BenchConfig) -> AmazonS3 {
    AmazonS3Builder::new()
        .with_bucket_name(&cfg.s3_bucket)
        .with_endpoint(&cfg.s3_endpoint)
        .with_access_key_id(&cfg.s3_access_key)
        .with_secret_access_key(&cfg.s3_secret_key)
        .with_region(&cfg.s3_region)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("failed to build S3 object store")
}

fn dataset_prefix(cfg: &BenchConfig, run_id: &str) -> String {
    format!("{}/{}/scalar", cfg.parquet_prefix, run_id)
}

fn dataset_uri(cfg: &BenchConfig, run_id: &str) -> String {
    // The trailing slash marks the listing table root as a directory; without
    // it DataFusion treats the path as a single Parquet file.
    format!("s3://{}/{}/", cfg.s3_bucket, dataset_prefix(cfg, run_id))
}

fn scalar_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("blob", DataType::Binary, false),
    ]))
}

fn scalar_batch(schema: SchemaRef, start: usize, rows: usize) -> RecordBatch {
    let ids: Vec<i32> = (start..start + rows).map(|row| row as i32).collect();
    let payloads: Vec<String> = (start..start + rows).map(scalar_payload).collect();
    let mut blobs = BinaryBuilder::with_capacity(rows, rows * SCALAR_BLOB_LEN);
    for row in start..start + rows {
        blobs.append_value(scalar_blob(row));
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
            Arc::new(blobs.finish()) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn ingest(cfg: &BenchConfig, run_id: &str) {
    let store = s3_store(cfg);
    let prefix = dataset_prefix(cfg, run_id);
    let schema = scalar_schema();
    let files = cfg.scalar_rows.div_ceil(cfg.inline_limit);

    let start = Instant::now();
    for file in 0..files {
        let file_start = file * cfg.inline_limit;
        let file_rows = (cfg.scalar_rows - file_start).min(cfg.inline_limit);

        let mut buffer = Vec::new();
        {
            let properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(
                    ZstdLevel::try_new(3).expect("valid zstd level"),
                ))
                .set_max_row_group_row_count(Some(cfg.row_group))
                .build();
            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(properties))
                .expect("failed to create parquet writer");

            let mut row = file_start;
            while row < file_start + file_rows {
                let batch_rows = (file_start + file_rows - row).min(cfg.scalar_batch);
                writer
                    .write(&scalar_batch(schema.clone(), row, batch_rows))
                    .expect("failed to write parquet batch");
                row += batch_rows;
            }
            writer.close().expect("failed to close parquet writer");
        }

        let path = ObjectPath::from(format!("{prefix}/part-{file:05}.parquet"));
        store
            .put(&path, buffer.into())
            .await
            .expect("failed to upload parquet file");
    }
    let elapsed = start.elapsed();

    emit(
        "datafusion",
        "ingest",
        &[
            ("table", "scalar".to_string()),
            ("op", "bulk_insert".to_string()),
            ("rows", cfg.scalar_rows.to_string()),
            ("files", files.to_string()),
            ("prefix", prefix),
            ("total_ms", elapsed.as_millis().to_string()),
            (
                "rows_per_s",
                format!("{:.0}", rows_per_second(cfg.scalar_rows, elapsed)),
            ),
            (
                "mib_per_s",
                format!(
                    "{:.1}",
                    mib_per_second(scalar_raw_bytes(cfg.scalar_rows), elapsed)
                ),
            ),
        ],
    );
}

async fn context(cfg: &BenchConfig, run_id: &str, target_partitions: usize) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(target_partitions);
    let ctx = SessionContext::new_with_config(config);

    let bucket_url = Url::parse(&format!("s3://{}", cfg.s3_bucket)).expect("valid bucket URL");
    ctx.register_object_store(&bucket_url, Arc::new(s3_store(cfg)));

    let options = ParquetReadOptions {
        parquet_pruning: Some(true),
        ..Default::default()
    };
    ctx.register_parquet("scalar", dataset_uri(cfg, run_id), options)
        .await
        .expect("failed to register parquet listing table");
    ctx
}

async fn full_scan(ctx: &SessionContext) -> (Duration, usize) {
    let start = Instant::now();
    let dataframe = ctx
        .sql("SELECT id, payload, blob FROM scalar")
        .await
        .expect("failed to plan full scan");
    let mut stream = dataframe
        .execute_stream()
        .await
        .expect("failed to execute full scan");
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("full scan batch failed").num_rows();
    }
    (start.elapsed(), rows)
}

async fn range_scan(ctx: &SessionContext, range_start: i32, range_end: i32) -> (Duration, usize) {
    let start = Instant::now();
    let sql = format!("SELECT id FROM scalar WHERE id >= {range_start} AND id < {range_end}");
    let dataframe = ctx.sql(&sql).await.expect("failed to plan range scan");
    let mut stream = dataframe
        .execute_stream()
        .await
        .expect("failed to execute range scan");
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("range scan batch failed").num_rows();
    }
    (start.elapsed(), rows)
}

async fn full_scan_phase(ctx: &SessionContext, cfg: &BenchConfig, mode: &str, partitions: usize) {
    let mut runs = Vec::new();
    let mut rows = 0;
    for _ in 0..3 {
        let (elapsed, scanned) = full_scan(ctx).await;
        assert_eq!(scanned, cfg.scalar_rows);
        rows = scanned;
        runs.push(elapsed);
    }
    let best = runs.iter().min().copied().unwrap();
    emit(
        "datafusion",
        "query",
        &[
            ("table", "scalar".to_string()),
            ("op", "full_scan".to_string()),
            ("mode", mode.to_string()),
            ("partitions", partitions.to_string()),
            (
                "runs_ms",
                format!(
                    "{:?}",
                    runs.iter().map(|d| d.as_millis()).collect::<Vec<_>>()
                ),
            ),
            ("best_ms", format!("{:.1}", best.as_secs_f64() * 1000.0)),
            ("rows", rows.to_string()),
            ("rows_per_s", format!("{:.0}", rows_per_second(rows, best))),
            (
                "mib_per_s",
                format!("{:.1}", mib_per_second(scalar_raw_bytes(rows), best)),
            ),
        ],
    );
}

async fn query_scalar(cfg: &BenchConfig, run_id: &str) {
    let single = context(cfg, run_id, 1).await;
    full_scan_phase(&single, cfg, "single", 1).await;

    let parallel = context(cfg, run_id, cfg.concurrency).await;
    full_scan_phase(&parallel, cfg, "parallel", cfg.concurrency).await;

    let ctx = single;

    assert!(
        cfg.range_rows > 0 && cfg.range_rows <= cfg.scalar_rows,
        "PERF_RANGE_ROWS must be in 1..=PERF_SCALAR_ROWS"
    );
    for sample in 0..20 {
        let start = range_start(cfg.scalar_rows, cfg.range_rows, sample) as i32;
        let (_, returned) = range_scan(&ctx, start, start + cfg.range_rows as i32).await;
        assert_eq!(
            returned, cfg.range_rows,
            "range warm-up returned unexpected rows for start={start}"
        );
    }
    let mut samples = Vec::with_capacity(cfg.range_queries);
    for sample in 0..cfg.range_queries {
        let start = range_start(cfg.scalar_rows, cfg.range_rows, sample) as i32;
        let (elapsed, returned) = range_scan(&ctx, start, start + cfg.range_rows as i32).await;
        assert_eq!(
            returned, cfg.range_rows,
            "range scan returned unexpected rows for start={start}"
        );
        samples.push(elapsed.as_secs_f64() * 1000.0);
    }
    let stats = stats(samples);
    emit(
        "datafusion",
        "query",
        &[
            ("table", "scalar".to_string()),
            ("op", "range_scan".to_string()),
            ("range_rows", cfg.range_rows.to_string()),
            (
                "selectivity",
                format!("{:.4}", cfg.range_rows as f64 / cfg.scalar_rows as f64),
            ),
            ("p50_ms", format!("{:.3}", stats.p50)),
            ("p95_ms", format!("{:.3}", stats.p95)),
            ("mean_ms", format!("{:.3}", stats.mean)),
            ("min_ms", format!("{:.3}", stats.min)),
            ("max_ms", format!("{:.3}", stats.max)),
            ("n", cfg.range_queries.to_string()),
            ("rows_per_query", cfg.range_rows.to_string()),
            (
                "rows_per_s",
                format!("{:.0}", cfg.range_rows as f64 / (stats.p50 / 1000.0)),
            ),
        ],
    );
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = parse_cli("datafusion-bench", USAGE);
    let cfg = BenchConfig::from_env();

    match cli.phase.as_str() {
        "ingest" => ingest(&cfg, &cli.run_id).await,
        "query" | "query-scalar" => query_scalar(&cfg, &cli.run_id).await,
        other => panic!("unknown phase '{other}'\nusage: datafusion-bench {USAGE}"),
    }
}

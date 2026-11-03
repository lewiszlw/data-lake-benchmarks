// SPDX-License-Identifier: MIT

//! iceberg-rust baseline: bulk write, single-row write, full scan, range scan.
//!
//! iceberg-rust 0.10.1 has no public transaction action for row-level updates
//! or deletes, so this adapter intentionally does not benchmark them.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, BinaryBuilder, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use data_lake_benchmarks::{
    emit, mib_per_second, parse_cli, range_start, rows_per_second, scalar_blob, scalar_payload,
    scalar_raw_bytes, stats, BenchConfig, SCALAR_BLOB_LEN,
};
use futures::StreamExt;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, FieldMatchMode};
use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA,
    S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{DataFileFormat, Datum, TableMetadata};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, Runtime, TableCreation, TableIdent,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

const USAGE: &str = "<ingest|query|query-scalar> [run-id]";
const INSERT_WARMUPS: usize = 5;
const NAMESPACE: &str = "bench";

type DataWriter =
    DataFileWriter<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

fn warehouse(cfg: &BenchConfig, run_id: &str) -> String {
    format!("s3://{}/{}/{}", cfg.s3_bucket, cfg.iceberg_prefix, run_id)
}

fn file_io(cfg: &BenchConfig) -> FileIO {
    FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }))
    .with_prop(S3_ENDPOINT, &cfg.s3_endpoint)
    .with_prop(S3_ACCESS_KEY_ID, &cfg.s3_access_key)
    .with_prop(S3_SECRET_ACCESS_KEY, &cfg.s3_secret_key)
    .with_prop(S3_REGION, &cfg.s3_region)
    .with_prop(S3_PATH_STYLE_ACCESS, "true")
    .with_prop(S3_DISABLE_CONFIG_LOAD, "true")
    .with_prop(S3_DISABLE_EC2_METADATA, "true")
    .build()
}

async fn catalog(cfg: &BenchConfig, warehouse: &str) -> MemoryCatalog {
    let props: HashMap<String, String> = [
        (MEMORY_CATALOG_WAREHOUSE, warehouse.to_string()),
        (S3_ENDPOINT, cfg.s3_endpoint.clone()),
        (S3_ACCESS_KEY_ID, cfg.s3_access_key.clone()),
        (S3_SECRET_ACCESS_KEY, cfg.s3_secret_key.clone()),
        (S3_REGION, cfg.s3_region.clone()),
        (S3_PATH_STYLE_ACCESS, "true".to_string()),
        (S3_DISABLE_CONFIG_LOAD, "true".to_string()),
        (S3_DISABLE_EC2_METADATA, "true".to_string()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect();

    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("iceberg", props)
        .await
        .expect("failed to build memory catalog")
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

fn scalar_batches(schema: SchemaRef, cfg: &BenchConfig) -> Vec<RecordBatch> {
    (0..cfg.scalar_rows / cfg.scalar_batch)
        .map(|i| scalar_batch(schema.clone(), i * cfg.scalar_batch, cfg.scalar_batch))
        .collect()
}

struct WriterSetup {
    properties: WriterProperties,
    locations: DefaultLocationGenerator,
    names: DefaultFileNameGenerator,
}

impl WriterSetup {
    fn new(table: &Table, cfg: &BenchConfig, prefix: &str) -> Self {
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(3).expect("valid zstd level"),
            ))
            .set_max_row_group_row_count(Some(cfg.row_group))
            .build();
        Self {
            properties,
            locations: DefaultLocationGenerator::new(table.metadata())
                .expect("failed to build data location generator"),
            names: DefaultFileNameGenerator::new(prefix.to_string(), None, DataFileFormat::Parquet),
        }
    }

    async fn writer(&self, table: &Table) -> DataWriter {
        let parquet = ParquetWriterBuilder::new_with_match_mode(
            self.properties.clone(),
            table.metadata().current_schema().clone(),
            FieldMatchMode::Name,
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            self.locations.clone(),
            self.names.clone(),
        );
        DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .expect("failed to build data file writer")
    }
}

async fn create_table(
    catalog: &MemoryCatalog,
    namespace: &NamespaceIdent,
    warehouse: &str,
    name: &str,
    schema: iceberg::spec::Schema,
) -> Table {
    let creation = TableCreation::builder()
        .name(name.to_string())
        .location(format!("{warehouse}/{name}"))
        .schema(schema)
        .build();
    catalog
        .create_table(namespace, creation)
        .await
        .expect("failed to create iceberg table")
}

async fn bulk_load(
    catalog: &MemoryCatalog,
    table: Table,
    setup: &WriterSetup,
    batches: &[RecordBatch],
    batches_per_file: usize,
) -> (Table, Duration, usize) {
    let start = Instant::now();
    let mut data_files = Vec::new();
    for chunk in batches.chunks(batches_per_file.max(1)) {
        let mut writer = setup.writer(&table).await;
        for batch in chunk {
            writer
                .write(batch.clone())
                .await
                .expect("failed to write data batch");
        }
        data_files.extend(writer.close().await.expect("failed to close data writer"));
    }
    let file_count = data_files.len();

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).expect("failed to apply fast append");
    let table = tx
        .commit(catalog)
        .await
        .expect("failed to commit bulk write");
    (table, start.elapsed(), file_count)
}

async fn single_write(
    catalog: &MemoryCatalog,
    table: &Table,
    setup: &WriterSetup,
    batch: RecordBatch,
) -> Table {
    let mut writer = setup.writer(table).await;
    writer
        .write(batch)
        .await
        .expect("failed to write single row");
    let data_files = writer
        .close()
        .await
        .expect("failed to close single-row writer");

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).expect("failed to apply fast append");
    tx.commit(catalog)
        .await
        .expect("failed to commit single-row write")
}

async fn write_marker(table: &Table, warehouse: &str, name: &str) {
    let location = table
        .metadata_location_result()
        .expect("table has no metadata location");
    let marker = format!("{warehouse}/_markers/{name}");
    table
        .file_io()
        .new_output(&marker)
        .expect("failed to open metadata marker")
        .write(Bytes::from(location.as_bytes().to_vec()))
        .await
        .expect("failed to write metadata marker");
}

async fn ingest(cfg: &BenchConfig, run_id: &str) {
    assert!(
        cfg.single_inserts > 0,
        "PERF_SINGLE_INSERTS must be greater than zero"
    );
    let warehouse = warehouse(cfg, run_id);
    let catalog = catalog(cfg, &warehouse).await;
    let namespace = NamespaceIdent::new(NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("failed to create namespace");

    let arrow_schema = scalar_schema();
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
        .expect("failed to convert arrow schema");
    let batches = scalar_batches(arrow_schema.clone(), cfg);
    let batches_per_file = (cfg.inline_limit / cfg.scalar_batch).max(1);

    // Bulk table: 1M rows in one commit.
    let table = create_table(
        &catalog,
        &namespace,
        &warehouse,
        "scalar",
        iceberg_schema.clone(),
    )
    .await;
    let setup = WriterSetup::new(&table, cfg, "data");
    let (table, bulk_elapsed, bulk_files) =
        bulk_load(&catalog, table, &setup, &batches, batches_per_file).await;
    write_marker(&table, &warehouse, "scalar").await;

    emit(
        "iceberg",
        "ingest",
        &[
            ("table", "scalar".to_string()),
            ("op", "bulk_write".to_string()),
            ("rows", cfg.scalar_rows.to_string()),
            ("files", bulk_files.to_string()),
            ("warehouse", warehouse.clone()),
            ("total_ms", bulk_elapsed.as_millis().to_string()),
            (
                "rows_per_s",
                format!("{:.0}", rows_per_second(cfg.scalar_rows, bulk_elapsed)),
            ),
            (
                "mib_per_s",
                format!(
                    "{:.1}",
                    mib_per_second(scalar_raw_bytes(cfg.scalar_rows), bulk_elapsed)
                ),
            ),
        ],
    );

    // Single-row table: same 1M-row seed, then one commit per row.
    let single_table = create_table(
        &catalog,
        &namespace,
        &warehouse,
        "single_insert",
        iceberg_schema,
    )
    .await;
    let seed_setup = WriterSetup::new(&single_table, cfg, "seed");
    let (mut single_table, _, _) = bulk_load(
        &catalog,
        single_table,
        &seed_setup,
        &batches,
        batches_per_file,
    )
    .await;

    let append_setup = WriterSetup::new(&single_table, cfg, "append");
    let mut next_id = cfg.scalar_rows;
    for _ in 0..INSERT_WARMUPS {
        let batch = scalar_batch(arrow_schema.clone(), next_id, 1);
        single_table = single_write(&catalog, &single_table, &append_setup, batch).await;
        next_id += 1;
    }

    let mut samples = Vec::with_capacity(cfg.single_inserts);
    for _ in 0..cfg.single_inserts {
        let batch = scalar_batch(arrow_schema.clone(), next_id, 1);
        let start = Instant::now();
        single_table = single_write(&catalog, &single_table, &append_setup, batch).await;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        next_id += 1;
    }
    write_marker(&single_table, &warehouse, "single_insert").await;

    let expected_rows = cfg.scalar_rows + INSERT_WARMUPS + cfg.single_inserts;
    let (_, scanned) = scan_rows(&single_table, 1).await;
    assert_eq!(
        scanned, expected_rows,
        "single-row writes did not all become visible"
    );
    let probe = (next_id - 1) as i32;
    let payload = read_payload(&single_table, probe).await;
    assert_eq!(
        payload.as_deref(),
        Some(scalar_payload(probe as usize).as_str()),
        "payload mismatch for id={probe}"
    );

    let mean_ms = samples.iter().sum::<f64>() / samples.len() as f64;
    emit(
        "iceberg",
        "ingest",
        &[
            ("table", "single_insert".to_string()),
            ("op", "single_write".to_string()),
            ("n", cfg.single_inserts.to_string()),
            ("warmups", INSERT_WARMUPS.to_string()),
            ("mean_ms", format!("{:.3}", mean_ms)),
            ("rows_per_s", format!("{:.1}", 1000.0 / mean_ms)),
        ],
    );

    emit(
        "iceberg",
        "ingest",
        &[
            ("table", "single_insert".to_string()),
            ("op", "state".to_string()),
            (
                "snapshots",
                single_table.metadata().snapshots().len().to_string(),
            ),
            (
                "metadata_location",
                single_table
                    .metadata_location_result()
                    .unwrap_or_default()
                    .to_string(),
            ),
        ],
    );
}

async fn open_table(cfg: &BenchConfig, run_id: &str) -> Table {
    let file_io = file_io(cfg);
    let warehouse = warehouse(cfg, run_id);
    let marker = format!("{warehouse}/_markers/scalar");
    let location = String::from_utf8(
        file_io
            .new_input(&marker)
            .expect("failed to open metadata marker")
            .read()
            .await
            .expect("failed to read metadata marker")
            .to_vec(),
    )
    .expect("metadata marker is not UTF-8");
    let metadata = TableMetadata::read_from(&file_io, &location)
        .await
        .expect("failed to read table metadata");

    Table::builder()
        .file_io(file_io)
        .metadata(metadata)
        .metadata_location(location)
        .identifier(TableIdent::from_strs([NAMESPACE, "scalar"]).expect("invalid table ident"))
        .runtime(Runtime::current())
        .build()
        .expect("failed to build table")
}

async fn scan_rows(table: &Table, concurrency: usize) -> (Duration, usize) {
    let start = Instant::now();
    let mut stream = table
        .scan()
        .with_concurrency_limit(concurrency)
        .with_batch_size(Some(8192))
        .build()
        .expect("failed to build full scan")
        .to_arrow()
        .await
        .expect("failed to plan full scan");
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("full scan batch failed").num_rows();
    }
    (start.elapsed(), rows)
}

async fn range_scan(table: &Table, start_id: i32, end_id: i32) -> (Duration, usize) {
    let start = Instant::now();
    let predicate = iceberg::expr::Reference::new("id")
        .greater_than_or_equal_to(Datum::int(start_id))
        .and(iceberg::expr::Reference::new("id").less_than(Datum::int(end_id)));
    let mut stream = table
        .scan()
        .with_filter(predicate)
        .select(["id"])
        .with_concurrency_limit(1)
        .with_batch_size(Some(8192))
        .build()
        .expect("failed to build range scan")
        .to_arrow()
        .await
        .expect("failed to plan range scan");
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("range scan batch failed").num_rows();
    }
    (start.elapsed(), rows)
}

async fn read_payload(table: &Table, id: i32) -> Option<String> {
    let predicate = iceberg::expr::Reference::new("id").equal_to(Datum::int(id));
    let mut stream = table
        .scan()
        .with_filter(predicate)
        .select(["id", "payload"])
        .with_concurrency_limit(1)
        .build()
        .expect("failed to build payload scan")
        .to_arrow()
        .await
        .expect("failed to plan payload scan");
    let mut value = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("payload scan batch failed");
        if batch.num_rows() == 0 {
            continue;
        }
        let payloads = batch
            .column_by_name("payload")
            .expect("payload column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column must be Utf8");
        value = Some(payloads.value(0).to_string());
    }
    value
}

async fn query_scalar(cfg: &BenchConfig, run_id: &str) {
    let table = open_table(cfg, run_id).await;

    for (mode, partitions) in [("single", 1usize), ("parallel", cfg.concurrency)] {
        let mut runs = Vec::new();
        let mut rows = 0;
        for _ in 0..3 {
            let (elapsed, scanned) = scan_rows(&table, partitions).await;
            assert_eq!(scanned, cfg.scalar_rows);
            rows = scanned;
            runs.push(elapsed);
        }
        let best = runs.iter().min().copied().unwrap();
        emit(
            "iceberg",
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

    assert!(
        cfg.range_rows > 0 && cfg.range_rows <= cfg.scalar_rows,
        "PERF_RANGE_ROWS must be in 1..=PERF_SCALAR_ROWS"
    );
    for sample in 0..20 {
        let start = range_start(cfg.scalar_rows, cfg.range_rows, sample) as i32;
        let (_, returned) = range_scan(&table, start, start + cfg.range_rows as i32).await;
        assert_eq!(
            returned, cfg.range_rows,
            "range warm-up returned unexpected rows for start={start}"
        );
    }
    let mut samples = Vec::with_capacity(cfg.range_queries);
    for sample in 0..cfg.range_queries {
        let start = range_start(cfg.scalar_rows, cfg.range_rows, sample) as i32;
        let (elapsed, returned) = range_scan(&table, start, start + cfg.range_rows as i32).await;
        assert_eq!(
            returned, cfg.range_rows,
            "range scan returned unexpected rows for start={start}"
        );
        samples.push(elapsed.as_secs_f64() * 1000.0);
    }
    let stats = stats(samples);
    emit(
        "iceberg",
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
    let cli = parse_cli("iceberg-bench", USAGE);
    let cfg = BenchConfig::from_env();

    match cli.phase.as_str() {
        "ingest" => ingest(&cfg, &cli.run_id).await,
        "query" | "query-scalar" => query_scalar(&cfg, &cli.run_id).await,
        other => panic!("unknown phase '{other}'\nusage: iceberg-bench {USAGE}"),
    }
}

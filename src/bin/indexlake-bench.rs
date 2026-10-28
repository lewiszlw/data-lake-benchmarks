// SPDX-License-Identifier: MIT

//! IndexLake phases: ingest, index, query.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeListBuilder, Float32Builder, Int32Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use data_lake_benchmarks::{
    emit, ground_truth, mib_per_second, mutation_bucket, mutation_bucket_rows,
    mutation_first_row_outside_bucket, mutation_payload, mutation_sample_ids,
    mutation_window_buckets, parse_cli, query_vector, rows_per_second, scalar_blob, scalar_payload,
    scalar_raw_bytes, splitmix64, stats, vector_raw_bytes, BenchConfig, QUERY_SEED,
};
use futures::StreamExt;
use indexlake::catalog::Scalar;
use indexlake::expr::{col, lit};
use indexlake::index::IndexKind;
use indexlake::storage::DataFileFormat;
use indexlake::table::{
    IndexCreation, Table, TableConfig, TableCreation, TableInsertion, TableScan,
    TableScanPartition, TableSearch, TableUpdate,
};
use indexlake::{Client, ILError};
use indexlake_catalog_postgres::PostgresCatalogBuilder;
use indexlake_index_hnsw::{HnswIndexKind, HnswIndexParams, HnswSearchQuery};
use indexlake_storage_s3::S3Storage;
use opendal::services::S3Config;

const USAGE: &str =
    "<ingest-bulk|ingest-insert|index|query|query-scalar|query-vector|mutate-conditional> [run-id]";

async fn client(cfg: &BenchConfig) -> Client {
    let catalog = Arc::new(
        PostgresCatalogBuilder::new(
            cfg.pg_host.clone(),
            cfg.pg_port,
            cfg.pg_user.clone(),
            cfg.pg_password.clone(),
        )
        .dbname(cfg.pg_database.clone())
        .pool_max_size(100)
        .pool_idle_timeout(Some(Duration::from_secs(10)))
        .build()
        .await
        .expect("failed to build PostgreSQL catalog"),
    );

    let mut s3 = S3Config::default();
    s3.endpoint = Some(cfg.s3_endpoint.clone());
    s3.access_key_id = Some(cfg.s3_access_key.clone());
    s3.secret_access_key = Some(cfg.s3_secret_key.clone());
    s3.region = Some(cfg.s3_region.clone());
    s3.disable_config_load = true;
    s3.disable_ec2_metadata = true;
    let storage = Arc::new(S3Storage::new(s3, cfg.s3_bucket.clone()));

    let mut client = Client::new(catalog, storage);
    client.register_index_kind(Arc::new(HnswIndexKind));
    client
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
    let mut blobs =
        BinaryBuilder::with_capacity(rows, rows * data_lake_benchmarks::SCALAR_BLOB_LEN);
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

fn mutation_scalar_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("blob", DataType::Binary, false),
        Field::new("bucket", DataType::Int32, false),
    ]))
}

fn mutation_scalar_batch(schema: SchemaRef, start: usize, rows: usize) -> RecordBatch {
    let ids: Vec<i32> = (start..start + rows).map(|row| row as i32).collect();
    let payloads: Vec<String> = (start..start + rows).map(scalar_payload).collect();
    let buckets: Vec<i32> = (start..start + rows).map(mutation_bucket).collect();
    let mut blobs =
        BinaryBuilder::with_capacity(rows, rows * data_lake_benchmarks::SCALAR_BLOB_LEN);
    for row in start..start + rows {
        blobs.append_value(scalar_blob(row));
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(payloads)) as ArrayRef,
            Arc::new(blobs.finish()) as ArrayRef,
            Arc::new(Int32Array::from(buckets)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn vector_schema(dim: usize) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dim as i32,
            ),
            false,
        ),
    ]))
}

fn vector_batch(schema: SchemaRef, start: usize, rows: usize, dim: usize) -> RecordBatch {
    let ids: Vec<i32> = (start..start + rows).map(|row| row as i32).collect();
    let inner = Arc::new(Field::new("item", DataType::Float32, false));
    let mut builder =
        FixedSizeListBuilder::new(Float32Builder::new(), dim as i32).with_field(inner);
    for row in start..start + rows {
        for d in 0..dim {
            builder.values().append_value(data_lake_benchmarks::f32_at(
                data_lake_benchmarks::DATA_SEED,
                row,
                d,
            ));
        }
        builder.append(true);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(builder.finish()) as ArrayRef,
        ],
    )
    .unwrap()
}

fn table_config(cfg: &BenchConfig) -> TableConfig {
    TableConfig {
        inline_row_count_limit: cfg.inline_limit,
        parquet_row_group_size: cfg.row_group,
        preferred_data_file_format: DataFileFormat::ParquetV2,
    }
}

async fn create_table(
    client: &Client,
    cfg: &BenchConfig,
    namespace: &str,
    name: &str,
    schema: SchemaRef,
) -> Table {
    let creation = TableCreation {
        namespace_name: namespace.to_string(),
        table_name: name.to_string(),
        schema,
        default_values: HashMap::new(),
        config: table_config(cfg),
        if_not_exists: false,
    };
    client
        .create_table(creation)
        .await
        .expect("failed to create table");
    client
        .load_table(namespace, name)
        .await
        .expect("failed to load table")
}

async fn wait_for_files(table: &Table, expected: usize, timeout: Duration) -> Duration {
    let start = Instant::now();
    loop {
        if table.data_file_count().await.unwrap() == expected {
            return start.elapsed();
        }
        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for {expected} data files, last count {}",
                table.data_file_count().await.unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn scalar_batches(schema: SchemaRef, cfg: &BenchConfig) -> Vec<RecordBatch> {
    (0..cfg.scalar_rows / cfg.scalar_batch)
        .map(|i| scalar_batch(schema.clone(), i * cfg.scalar_batch, cfg.scalar_batch))
        .collect()
}

fn mutation_scalar_batches(schema: SchemaRef, cfg: &BenchConfig) -> Vec<RecordBatch> {
    (0..cfg.scalar_rows / cfg.scalar_batch)
        .map(|i| mutation_scalar_batch(schema.clone(), i * cfg.scalar_batch, cfg.scalar_batch))
        .collect()
}

async fn create_and_bulk_insert(
    client: &Client,
    cfg: &BenchConfig,
    namespace: &str,
    name: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> (Table, Duration) {
    let table = create_table(client, cfg, namespace, name, schema).await;
    let stream = futures::stream::iter(batches.into_iter().map(Ok::<_, ILError>)).boxed();

    let start = Instant::now();
    let count = table
        .bypass_insert(stream)
        .await
        .expect("bypass_insert failed");
    let elapsed = start.elapsed();
    assert_eq!(count, cfg.scalar_rows);
    assert_eq!(
        table.data_file_count().await.unwrap(),
        cfg.expected_files(cfg.scalar_rows)
    );
    (table, elapsed)
}

async fn ingest_bulk_scalar(client: &Client, cfg: &BenchConfig, namespace: &str) -> Table {
    let schema = scalar_schema();
    let batches = scalar_batches(schema.clone(), cfg);
    let (table, elapsed) =
        create_and_bulk_insert(client, cfg, namespace, "scalar", schema, batches).await;

    emit(
        "indexlake",
        "ingest",
        &[
            ("table", "scalar_bulk".to_string()),
            ("rows", cfg.scalar_rows.to_string()),
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
    table
}

async fn ingest_bulk_vector(client: &Client, cfg: &BenchConfig, namespace: &str) -> Table {
    let schema = vector_schema(cfg.dim);
    let table = create_table(client, cfg, namespace, "vector", schema.clone()).await;
    let batches: Vec<RecordBatch> = (0..cfg.vector_rows / cfg.vector_batch)
        .map(|i| {
            vector_batch(
                schema.clone(),
                i * cfg.vector_batch,
                cfg.vector_batch,
                cfg.dim,
            )
        })
        .collect();
    let stream = futures::stream::iter(batches.into_iter().map(Ok::<_, ILError>)).boxed();

    let start = Instant::now();
    let count = table
        .bypass_insert(stream)
        .await
        .expect("bypass_insert failed");
    let elapsed = start.elapsed();
    assert_eq!(count, cfg.vector_rows);
    assert_eq!(
        table.data_file_count().await.unwrap(),
        cfg.expected_files(cfg.vector_rows)
    );

    emit(
        "indexlake",
        "ingest",
        &[
            ("table", "vector_bulk".to_string()),
            ("rows", cfg.vector_rows.to_string()),
            ("total_ms", elapsed.as_millis().to_string()),
            (
                "rows_per_s",
                format!("{:.0}", rows_per_second(cfg.vector_rows, elapsed)),
            ),
            (
                "mib_per_s",
                format!(
                    "{:.1}",
                    mib_per_second(vector_raw_bytes(cfg.vector_rows, cfg.dim), elapsed)
                ),
            ),
        ],
    );
    table
}

async fn ingest_insert_scalar(client: &Client, cfg: &BenchConfig, namespace: &str) -> Table {
    let schema = scalar_schema();
    let table = create_table(client, cfg, namespace, "scalar_insert", schema.clone()).await;
    let batches: Vec<RecordBatch> = (0..cfg.scalar_rows / cfg.scalar_batch)
        .map(|i| scalar_batch(schema.clone(), i * cfg.scalar_batch, cfg.scalar_batch))
        .collect();

    let start = Instant::now();
    for batch in batches {
        table
            .insert(TableInsertion::new(vec![batch]))
            .await
            .expect("insert failed");
    }
    let api_elapsed = start.elapsed();
    let dump_elapsed = wait_for_files(
        &table,
        cfg.expected_files(cfg.scalar_rows),
        Duration::from_secs(30 * 60),
    )
    .await;
    let total = start.elapsed();

    emit(
        "indexlake",
        "ingest",
        &[
            ("table", "scalar_insert".to_string()),
            ("rows", cfg.scalar_rows.to_string()),
            ("api_ms", api_elapsed.as_millis().to_string()),
            ("dump_wait_ms", dump_elapsed.as_millis().to_string()),
            ("total_ms", total.as_millis().to_string()),
            (
                "rows_per_s",
                format!("{:.0}", rows_per_second(cfg.scalar_rows, total)),
            ),
        ],
    );
    table
}

async fn ingest_insert_vector(client: &Client, cfg: &BenchConfig, namespace: &str) -> Table {
    let schema = vector_schema(cfg.dim);
    let table = create_table(client, cfg, namespace, "vector_insert", schema.clone()).await;
    let batches: Vec<RecordBatch> = (0..cfg.vector_rows / cfg.vector_batch)
        .map(|i| {
            vector_batch(
                schema.clone(),
                i * cfg.vector_batch,
                cfg.vector_batch,
                cfg.dim,
            )
        })
        .collect();

    let start = Instant::now();
    for batch in batches {
        table
            .insert(TableInsertion::new(vec![batch]))
            .await
            .expect("insert failed");
    }
    let api_elapsed = start.elapsed();
    let dump_elapsed = wait_for_files(
        &table,
        cfg.expected_files(cfg.vector_rows),
        Duration::from_secs(30 * 60),
    )
    .await;
    let total = start.elapsed();

    emit(
        "indexlake",
        "ingest",
        &[
            ("table", "vector_insert".to_string()),
            ("rows", cfg.vector_rows.to_string()),
            ("api_ms", api_elapsed.as_millis().to_string()),
            ("dump_wait_ms", dump_elapsed.as_millis().to_string()),
            ("total_ms", total.as_millis().to_string()),
            (
                "rows_per_s",
                format!("{:.0}", rows_per_second(cfg.vector_rows, total)),
            ),
        ],
    );
    table
}

async fn create_vector_index(table: &Table, cfg: &BenchConfig) {
    let creation = IndexCreation {
        name: "vector_idx".to_string(),
        kind: HnswIndexKind.kind().to_string(),
        key_columns: vec!["vector".to_string()],
        params: Arc::new(HnswIndexParams {
            ef_construction: cfg.ef_construction,
        }),
        concurrency: 1,
        if_not_exists: false,
        config: Default::default(),
    };
    let start = Instant::now();
    table
        .clone()
        .create_index(creation)
        .await
        .expect("HNSW index build failed");
    emit(
        "indexlake",
        "index",
        &[
            ("table", "vector".to_string()),
            ("build_ms", start.elapsed().as_millis().to_string()),
        ],
    );
}

async fn full_scan(table: &Table, partition_count: usize) -> (Duration, usize) {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(partition_count);
    for partition_idx in 0..partition_count {
        let table = table.clone();
        handles.push(tokio::spawn(async move {
            let scan = TableScan::default()
                .with_partition(TableScanPartition::Auto {
                    partition_idx,
                    partition_count,
                })
                .with_batch_size(8192);
            let mut stream = table.scan(scan).await?;
            let mut rows = 0usize;
            while let Some(batch) = stream.next().await {
                rows += batch?.num_rows();
            }
            Ok::<_, ILError>(rows)
        }));
    }
    let mut rows = 0;
    for handle in handles {
        rows += handle.await.unwrap().unwrap();
    }
    (start.elapsed(), rows)
}

fn point_scan(id: i32) -> TableScan {
    TableScan::default()
        .with_filters(vec![col("id").eq(lit(Scalar::Int32(Some(id))))])
        .with_projection(Some(vec![1]))
        .with_batch_size(16)
}

async fn run_point_query(table: &Table, id: i32) -> (Duration, usize) {
    let start = Instant::now();
    let mut stream = table.scan(point_scan(id)).await.unwrap();
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    (start.elapsed(), rows)
}

async fn vector_search(table: &Table, cfg: &BenchConfig, vector: Vec<f32>) -> (Duration, Vec<i32>) {
    let search = TableSearch {
        query: Arc::new(HnswSearchQuery {
            vector,
            ef_search: cfg.ef_search,
        }),
        projection: Some(vec![1]),
        dynamic_fields: vec![],
        limit: Some(cfg.k),
        concurrency: cfg.concurrency,
    };
    let start = Instant::now();
    let mut stream = table.search(search).await.unwrap();
    let mut ids = Vec::with_capacity(cfg.k);
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let ids_array = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        ids.extend(ids_array.values().iter().copied());
    }
    (start.elapsed(), ids)
}

async fn query_scalar(client: &Client, cfg: &BenchConfig, namespace: &str) {
    let table = client.load_table(namespace, "scalar").await.unwrap();

    for (label, partitions) in [("single", 1usize), ("parallel", cfg.concurrency)] {
        let mut runs = Vec::new();
        let mut rows = 0;
        for _ in 0..3 {
            let (elapsed, scanned) = full_scan(&table, partitions).await;
            assert_eq!(scanned, cfg.scalar_rows);
            rows = scanned;
            runs.push(elapsed);
        }
        let best = runs.iter().min().copied().unwrap();
        emit(
            "indexlake",
            "query",
            &[
                ("table", "scalar".to_string()),
                ("op", "full_scan".to_string()),
                ("mode", label.to_string()),
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

    for id in 0..20 {
        let (_, rows) = run_point_query(&table, id).await;
        assert_eq!(rows, 1, "point query miss for id={id}");
    }
    let mut samples = Vec::with_capacity(cfg.point_queries);
    for i in 0..cfg.point_queries {
        let id = (splitmix64(QUERY_SEED ^ i as u64) % cfg.scalar_rows as u64) as i32;
        let (elapsed, rows) = run_point_query(&table, id).await;
        assert_eq!(rows, 1, "point query miss for id={id}");
        samples.push(elapsed.as_secs_f64() * 1000.0);
    }
    let stats = stats(samples);
    emit(
        "indexlake",
        "query",
        &[
            ("table", "scalar".to_string()),
            ("op", "point".to_string()),
            ("p50_ms", format!("{:.3}", stats.p50)),
            ("p95_ms", format!("{:.3}", stats.p95)),
            ("mean_ms", format!("{:.3}", stats.mean)),
            ("min_ms", format!("{:.3}", stats.min)),
            ("max_ms", format!("{:.3}", stats.max)),
            ("n", cfg.point_queries.to_string()),
        ],
    );
}

async fn read_payload(table: &Table, id: i32) -> Option<String> {
    // IndexLake puts the internal row id at schema index 0, so user columns
    // start at 1: id=1, payload=2, blob=3, bucket=4.
    let scan = TableScan::default()
        .with_filters(vec![col("id").eq(lit(Scalar::Int32(Some(id))))])
        .with_projection(Some(vec![2]))
        .with_batch_size(16);
    let mut stream = table.scan(scan).await.unwrap();
    let mut value = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        if batch.num_rows() == 0 {
            continue;
        }
        let payloads = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column must be Utf8");
        value = Some(payloads.value(0).to_string());
    }
    value
}

async fn assert_payload(table: &Table, id: i32, expected: &str) {
    let actual = read_payload(table, id).await;
    assert_eq!(
        actual.as_deref(),
        Some(expected),
        "payload mismatch for id={id}"
    );
}

async fn mutate_conditional_update(
    client: &Client,
    cfg: &BenchConfig,
    namespace: &str,
    buckets: &[i32],
) {
    let schema = mutation_scalar_schema();
    let batches = mutation_scalar_batches(schema.clone(), cfg);
    let (table, _) = create_and_bulk_insert(
        client,
        cfg,
        namespace,
        "scalar_update_cond",
        schema,
        batches,
    )
    .await;

    emit(
        "indexlake",
        "mutate",
        &[
            ("table", "scalar_update_cond".to_string()),
            ("op", "prepare".to_string()),
            (
                "data_files",
                table.data_file_count().await.unwrap().to_string(),
            ),
        ],
    );

    for (window, &bucket) in buckets.iter().enumerate() {
        let expected = mutation_bucket_rows(cfg.scalar_rows, bucket);
        assert!(expected > 0, "bucket {bucket} has no rows");
        let updated = mutation_payload(bucket);

        // Warm the metadata cache so the timed operation does not pay for the first open.
        let warm_id = mutation_sample_ids(cfg.scalar_rows, bucket, 1)[0];
        let (_, rows) = run_point_query(&table, warm_id).await;
        assert_eq!(rows, 1, "warm-up point query missed id={warm_id}");

        let update = TableUpdate {
            set_map: HashMap::from([(
                "payload".to_string(),
                lit(Scalar::Utf8(Some(updated.clone()))),
            )]),
            condition: col("bucket").eq(lit(bucket)),
        };

        let start = Instant::now();
        let affected = table
            .update(update)
            .await
            .expect("conditional update failed");
        let elapsed = start.elapsed();

        assert_eq!(
            affected, expected,
            "conditional update affected rows for bucket {bucket}"
        );
        for id in mutation_sample_ids(cfg.scalar_rows, bucket, 8) {
            assert_payload(&table, id, &updated).await;
        }
        let outside = mutation_first_row_outside_bucket(cfg.scalar_rows, bucket);
        assert_payload(&table, outside as i32, &scalar_payload(outside)).await;

        emit(
            "indexlake",
            "mutate",
            &[
                ("table", "scalar_update_cond".to_string()),
                ("op", "update_conditional".to_string()),
                ("window", window.to_string()),
                ("bucket", bucket.to_string()),
                ("rows_target", expected.to_string()),
                ("rows_affected", affected.to_string()),
                ("elapsed_ms", elapsed.as_millis().to_string()),
                (
                    "rows_per_s",
                    format!("{:.1}", rows_per_second(affected, elapsed)),
                ),
            ],
        );
    }

    emit(
        "indexlake",
        "mutate",
        &[
            ("table", "scalar_update_cond".to_string()),
            ("op", "state".to_string()),
            (
                "prefix",
                format!("{}/{}", table.namespace_id, table.table_id),
            ),
            (
                "data_files",
                table.data_file_count().await.unwrap().to_string(),
            ),
            (
                "inline_rows",
                table.inline_row_count().await.unwrap().to_string(),
            ),
        ],
    );
}

async fn mutate_conditional_delete(
    client: &Client,
    cfg: &BenchConfig,
    namespace: &str,
    buckets: &[i32],
) {
    let schema = mutation_scalar_schema();
    let batches = mutation_scalar_batches(schema.clone(), cfg);
    let (table, _) = create_and_bulk_insert(
        client,
        cfg,
        namespace,
        "scalar_delete_cond",
        schema,
        batches,
    )
    .await;

    emit(
        "indexlake",
        "mutate",
        &[
            ("table", "scalar_delete_cond".to_string()),
            ("op", "prepare".to_string()),
            (
                "data_files",
                table.data_file_count().await.unwrap().to_string(),
            ),
        ],
    );

    for (window, &bucket) in buckets.iter().enumerate() {
        let expected = mutation_bucket_rows(cfg.scalar_rows, bucket);
        assert!(expected > 0, "bucket {bucket} has no rows");

        let warm_id = mutation_sample_ids(cfg.scalar_rows, bucket, 1)[0];
        let (_, rows) = run_point_query(&table, warm_id).await;
        assert_eq!(rows, 1, "warm-up point query missed id={warm_id}");

        let start = Instant::now();
        let affected = table
            .delete(col("bucket").eq(lit(bucket)))
            .await
            .expect("conditional delete failed");
        let elapsed = start.elapsed();

        assert_eq!(
            affected, expected,
            "conditional delete affected rows for bucket {bucket}"
        );
        for id in mutation_sample_ids(cfg.scalar_rows, bucket, 8) {
            let (_, rows) = run_point_query(&table, id).await;
            assert_eq!(rows, 0, "deleted row id={id} is still visible");
        }
        let outside = mutation_first_row_outside_bucket(cfg.scalar_rows, bucket);
        let (_, rows) = run_point_query(&table, outside as i32).await;
        assert_eq!(rows, 1, "non-target row id={outside} was deleted");

        emit(
            "indexlake",
            "mutate",
            &[
                ("table", "scalar_delete_cond".to_string()),
                ("op", "delete_conditional".to_string()),
                ("window", window.to_string()),
                ("bucket", bucket.to_string()),
                ("rows_target", expected.to_string()),
                ("rows_affected", affected.to_string()),
                ("elapsed_ms", elapsed.as_millis().to_string()),
                (
                    "rows_per_s",
                    format!("{:.1}", rows_per_second(affected, elapsed)),
                ),
            ],
        );
    }

    emit(
        "indexlake",
        "mutate",
        &[
            ("table", "scalar_delete_cond".to_string()),
            ("op", "state".to_string()),
            (
                "prefix",
                format!("{}/{}", table.namespace_id, table.table_id),
            ),
            (
                "data_files",
                table.data_file_count().await.unwrap().to_string(),
            ),
            (
                "inline_rows",
                table.inline_row_count().await.unwrap().to_string(),
            ),
        ],
    );
}

async fn mutate_conditional(client: &Client, cfg: &BenchConfig, namespace: &str) {
    let buckets = mutation_window_buckets(cfg.mutation_bucket, cfg.mutation_windows);
    mutate_conditional_update(client, cfg, namespace, &buckets).await;
    mutate_conditional_delete(client, cfg, namespace, &buckets).await;
}

async fn query_vector_phase(client: &Client, cfg: &BenchConfig, namespace: &str) {
    let table = client.load_table(namespace, "vector").await.unwrap();

    let queries: Vec<Vec<f32>> = (0..cfg.recall_queries)
        .map(|q| query_vector(q, cfg.dim))
        .collect();
    let ground_truth_start = Instant::now();
    let truth = {
        let rows = cfg.vector_rows;
        let dim = cfg.dim;
        let k = cfg.k;
        tokio::task::spawn_blocking(move || ground_truth(&queries, rows, dim, k))
            .await
            .unwrap()
    };
    emit(
        "indexlake",
        "query",
        &[
            ("table", "vector".to_string()),
            ("op", "ground_truth".to_string()),
            ("ms", ground_truth_start.elapsed().as_millis().to_string()),
            ("queries", cfg.recall_queries.to_string()),
        ],
    );

    for q in 0..10 {
        let (_, ids) = vector_search(&table, cfg, query_vector(q, cfg.dim)).await;
        assert_eq!(ids.len(), cfg.k);
    }

    let mut samples = Vec::with_capacity(cfg.ann_queries);
    let mut hits = 0usize;
    let mut total = 0usize;
    for q in 0..cfg.ann_queries {
        let (elapsed, ids) = vector_search(&table, cfg, query_vector(q, cfg.dim)).await;
        samples.push(elapsed.as_secs_f64() * 1000.0);
        if q < cfg.recall_queries {
            let found: std::collections::HashSet<i32> = ids.iter().copied().collect();
            hits += truth[q].iter().filter(|id| found.contains(id)).count();
            total += cfg.k;
        }
    }
    let stats = stats(samples);
    emit(
        "indexlake",
        "query",
        &[
            ("table", "vector".to_string()),
            ("op", "ann".to_string()),
            ("p50_ms", format!("{:.3}", stats.p50)),
            ("p95_ms", format!("{:.3}", stats.p95)),
            ("mean_ms", format!("{:.3}", stats.mean)),
            ("min_ms", format!("{:.3}", stats.min)),
            ("max_ms", format!("{:.3}", stats.max)),
            ("n", cfg.ann_queries.to_string()),
            ("recall_at_k", format!("{:.4}", hits as f64 / total as f64)),
        ],
    );

    let per_task = cfg.ann_queries.div_ceil(cfg.concurrency);
    let total_queries = per_task * cfg.concurrency;
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.concurrency);
    for task in 0..cfg.concurrency {
        let table = table.clone();
        let cfg = cfg.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per_task {
                let query = task * per_task + i;
                let (_, ids) = vector_search(&table, &cfg, query_vector(query, cfg.dim)).await;
                assert_eq!(ids.len(), cfg.k);
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let elapsed = start.elapsed();
    emit(
        "indexlake",
        "query",
        &[
            ("table", "vector".to_string()),
            ("op", "ann_throughput".to_string()),
            ("concurrency", cfg.concurrency.to_string()),
            ("queries", total_queries.to_string()),
            ("elapsed_ms", elapsed.as_millis().to_string()),
            (
                "qps",
                format!("{:.1}", total_queries as f64 / elapsed.as_secs_f64()),
            ),
        ],
    );
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = parse_cli("indexlake-bench", USAGE);
    let cfg = BenchConfig::from_env();
    let namespace = cfg.namespace(&cli.run_id);
    let client = client(&cfg).await;

    match cli.phase.as_str() {
        "ingest-bulk" => {
            client.create_namespace(&namespace, true).await.unwrap();
            let scalar = ingest_bulk_scalar(&client, &cfg, &namespace).await;
            let vector = ingest_bulk_vector(&client, &cfg, &namespace).await;
            emit(
                "indexlake",
                "ingest",
                &[
                    ("run_id", cli.run_id.clone()),
                    ("namespace", namespace.clone()),
                    (
                        "scalar_prefix",
                        format!("{}/{}", scalar.namespace_id, scalar.table_id),
                    ),
                    (
                        "vector_prefix",
                        format!("{}/{}", vector.namespace_id, vector.table_id),
                    ),
                ],
            );
        }
        "ingest-insert" => {
            client.create_namespace(&namespace, true).await.unwrap();
            ingest_insert_scalar(&client, &cfg, &namespace).await;
            ingest_insert_vector(&client, &cfg, &namespace).await;
        }
        "index" => {
            let vector = client.load_table(&namespace, "vector").await.unwrap();
            create_vector_index(&vector, &cfg).await;
        }
        "query" => {
            query_scalar(&client, &cfg, &namespace).await;
            query_vector_phase(&client, &cfg, &namespace).await;
        }
        "query-scalar" => query_scalar(&client, &cfg, &namespace).await,
        "query-vector" => query_vector_phase(&client, &cfg, &namespace).await,
        "mutate-conditional" => {
            client.create_namespace(&namespace, true).await.unwrap();
            mutate_conditional(&client, &cfg, &namespace).await;
        }
        other => panic!("unknown phase '{other}'\nusage: indexlake-bench {USAGE}"),
    }
}

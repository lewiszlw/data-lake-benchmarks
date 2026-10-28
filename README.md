# Data Lake Benchmarks

Reproducible, S3-backed comparisons between [IndexLake](https://github.com/systemxlabs/indexlake)
and [Lance](https://github.com/lance-format/lance).

The benchmark covers four operations on the same logical data:

| Operation | Workload |
|---|---|
| Bulk ingest | 1M scalar rows and 100k × 1024-d vectors, no indexes |
| Full scan | Read every scalar column and count rows |
| Point lookup | Equality on a unique `Int32` key with a BTree index |
| Vector search | Top-10 ANN over 100k vectors with HNSW (L2) |

Storage bytes and compression ratios are measured directly from S3 object
sizes. Index build time is reported separately so it does not contaminate
ingest or query numbers.

See [docs/RESULTS.md](docs/RESULTS.md) for the measured results and analysis.

## Layout

```
crates/
  bench-core/       Deterministic workload, config, statistics, ground truth
  indexlake-bench/  IndexLake adapter and phases
  lance-bench/      Lance adapter and phases
scripts/
  run_all.sh        End-to-end orchestration
  s3_size.py        Stdlib-only S3 SigV4 size/list/delete helper
docs/
  RESULTS.md        Measured numbers, methodology notes, caveats
```

The engine crates intentionally depend on different Arrow major versions
(IndexLake uses Arrow 59, Lance uses Arrow 58), which is why data generation
lives in `bench-core` as Arrow-independent primitives and each adapter builds
its own record batches.

## Dependencies

Both engines are consumed directly from git. The revisions are pinned in the
workspace `Cargo.toml`:

- IndexLake: `6fe5c37e9dfdb5d7bfc91cc5a0d71f6b963e16e4`
- Lance: `51415518c7566c938ba14fe29bcb357b8adf2123`

Update both the dependency entries and [docs/RESULTS.md](docs/RESULTS.md) when
changing them.

## Prerequisites

- Rust 1.89 or newer.
- Python 3.8 or newer (only the stdlib is used by `scripts/s3_size.py`).
- An S3-compatible object store. The defaults target a local MinIO at
  `http://127.0.0.1:9000`, bucket `indexlake`.
- PostgreSQL for IndexLake's catalog. The schema is in IndexLake's
  [`init_catalog.sql`](https://github.com/systemxlabs/indexlake/blob/main/integration-tests/testdata/postgres/init_catalog.sql).
  Lance does not need PostgreSQL.
- The MinIO endpoint must not go through `HTTP_PROXY`. The scripts set
  `NO_PROXY=127.0.0.1,localhost` by default.

Example local services:

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=password \
  -e MINIO_DEFAULT_BUCKETS=indexlake minio/minio server /data --console-address :9001

docker run -d --name pg -p 5432:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password postgres
psql -h 127.0.0.1 -U postgres -d postgres \
  -f <path-to-indexlake>/integration-tests/testdata/postgres/init_catalog.sql
```

## Configuration

All settings have local defaults; override with environment variables.

| Variable | Default | Purpose |
|---|---|---|
| `S3_ENDPOINT` | `http://127.0.0.1:9000` | Object store endpoint |
| `S3_BUCKET` | `indexlake` | Bucket |
| `S3_ACCESS_KEY` | `admin` | Access key |
| `S3_SECRET_KEY` | `password` | Secret key |
| `S3_REGION` | `us-east-1` | Region |
| `PGHOST` | `localhost` | IndexLake catalog host |
| `PGPORT` | `5432` | Catalog port |
| `PGUSER` | `postgres` | Catalog user |
| `PGPASSWORD` | `password` | Catalog password |
| `PGDATABASE` | `postgres` | Catalog database |
| `PERF_SCALAR_ROWS` | `1000000` | Scalar row count |
| `PERF_SCALAR_BATCH` | `10000` | Scalar rows per batch |
| `PERF_INLINE_LIMIT` | `100000` | IndexLake inline limit / target rows per data file |
| `PERF_ROW_GROUP` | `10000` | Rows per Parquet/Lance row group |
| `PERF_VECTOR_ROWS` | `100000` | Vector row count |
| `PERF_VECTOR_BATCH` | `1000` | Vectors per batch |
| `PERF_DIM` | `1024` | Vector dimension |
| `PERF_K` | `10` | ANN results |
| `PERF_EF_SEARCH` | `256` | HNSW search beam |
| `PERF_EF_CONSTRUCTION` | `400` | HNSW construction beam |
| `PERF_POINT_QUERIES` | `200` | Point-query latency samples |
| `PERF_ANN_QUERIES` | `100` | ANN latency samples |
| `PERF_RECALL_QUERIES` | `20` | Queries used for recall@10 |
| `PERF_CONCURRENCY` | `8` | Scan partitions and ANN client concurrency |
| `PERF_LANCE_PREFIX` | `perf-eval/lance` | Prefix under the bucket for Lance datasets |

## Running

```bash
cargo build --release

# Full pipeline: ingest, sizes, index, sizes, queries, sizes/probe.
scripts/run_all.sh my-run

# Add IndexLake's transactional insert path for comparison.
RUN_NATIVE_INSERT=1 scripts/run_all.sh my-run

# Individual phases.
target/release/indexlake-bench ingest-bulk my-run
target/release/indexlake-bench index my-run
target/release/indexlake-bench query-scalar my-run
target/release/indexlake-bench query-vector my-run
target/release/indexlake-bench probe my-run

target/release/lance-bench ingest my-run
target/release/lance-bench index my-run
target/release/lance-bench query-scalar my-run
target/release/lance-bench query-vector my-run
```

`run_all.sh` writes logs and size snapshots to `results/<run-id>/`.

Measuring a prefix manually:

```bash
python3 scripts/s3_size.py size <namespace-id>            # IndexLake
python3 scripts/s3_size.py size perf-eval/lance/<run-id>  # Lance
```

## Methodology Notes

- **Bulk ingest** uses IndexLake's `bypass_insert`, which streams Parquet data
  files and commits once, bypassing PostgreSQL inline staging. This is the
  fair counterpart to Lance's `Dataset::write`. `ingest-insert` additionally
  measures IndexLake's transactional insert path.
- **No index maintenance** is included in ingest: tables are created without
  indexes, and index builds run in a separate phase.
- **Queries are warm-cache**: warmups are excluded, then p50/p95 are reported.
  The full-scan phase reports all three runs and the best one.
- **Compression** is reported as `stored bytes / logical raw bytes`; values
  below 1 mean compression. Raw scalar bytes include 4-byte id, 128-byte text,
  and 128-byte binary per row. Raw vector bytes include 4-byte id and
  1024 × 4 bytes per vector.
- **ANN quality matters**: latency alone is not comparable when recall
  differs. The harness reports recall@10 against an exact squared-L2 scan over
  the same generated vectors.

### Fairness caveats

- MinIO is a local shared instance. Network and disk contention can dominate
  short runs; use paired runs and medians.
- The vector dataset is uniform random, which is intentionally hard for ANN.
  Absolute recall is not representative of clustered real-world data.
- IndexLake and Lance use different HNSW implementations and graph parameters,
  and IndexLake currently builds one index per data file. Results reflect the
  pinned revisions, not a tuned configuration.
- IndexLake query latency includes PostgreSQL catalog round trips. Lance keeps
  metadata and indexes in memory through its dataset/index caches.

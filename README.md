# Data Lake Benchmarks

Reproducible, S3-backed comparisons between [IndexLake](https://github.com/systemxlabs/indexlake)
and [Lance](https://github.com/lance-format/lance).

Both engines are consumed directly from git and pinned to revisions that use
**Arrow 58**, so the lockfile resolves a single Arrow version.

| Operation | Workload |
|---|---|
| Bulk ingest | 1M scalar rows and 100k × 1024-d vectors, no indexes |
| Full scan | Read every scalar column and count rows |
| Range scan | Contiguous `id ∈ [start, start + 10k)` over 1M rows, **no secondary index** |
| Vector search | Top-10 ANN over 100k vectors with HNSW (L2) |
| Conditional update | `bucket = id % 100` matches 1% of scalar rows; set `payload` to a fixed 128-byte value |
| Conditional delete | `bucket = id % 100` matches 1% of scalar rows |

Storage bytes and compression ratios are measured from S3 object sizes. Index
build time is measured separately so it does not contaminate ingest or query
numbers. The results are embedded below.

## Layout

The repository is one Cargo package with two binaries. Engine-independent
code lives in the library; each binary is an adapter.

```
Cargo.toml
src/
  lib.rs            Shared workload/config/statistics/ground truth
  cli.rs            Phase and run-id parsing, result format
  config.rs         Environment-driven configuration
  data.rs           Deterministic data generation and exact ground truth
  stats.rs          Latency and throughput helpers
  bin/
    indexlake-bench.rs
    lance-bench.rs
scripts/
  run_all.sh                  End-to-end orchestration
  s3_size.py                  Stdlib-only S3 SigV4 size/list/delete helper
  init-indexlake-catalog.sql  PostgreSQL schema matching the pinned IndexLake rev
```

## Dependencies

| Engine | Revision | Arrow |
|---|---|---|
| IndexLake | `4e3ff351ac4c5a05ce4d469b91f48bf883d75e87` (`v0.5.0`) | 58 |
| Lance | `51415518c7566c938ba14fe29bcb357b8adf2123` | 58 |

The pins are in `Cargo.toml`. Update them together with the results below.

## Prerequisites

- Rust 1.89 or newer.
- Python 3.8 or newer (only the standard library is used by `scripts/s3_size.py`).
- An S3-compatible object store. Defaults target local MinIO at
  `http://127.0.0.1:9000`, bucket `indexlake`.
- PostgreSQL for IndexLake's catalog. Use the schema from the pinned IndexLake
  revision, copied into `scripts/init-indexlake-catalog.sql`. Newer IndexLake
  revisions changed the inline-index schema; using their SQL with this pin can
  fail on vector search.
- The MinIO endpoint must bypass `HTTP_PROXY`. `run_all.sh` appends
  `127.0.0.1,localhost` to `NO_PROXY`.

Example local services:

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=password \
  -e MINIO_DEFAULT_BUCKETS=indexlake minio/minio server /data --console-address :9001

docker run -d --name pg -p 5432:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password postgres
psql -h 127.0.0.1 -U postgres -d postgres -f scripts/init-indexlake-catalog.sql
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
| `PERF_RANGE_QUERIES` | `200` | Range-scan latency samples |
| `PERF_RANGE_ROWS` | `10000` | Rows returned by each range scan |
| `PERF_ANN_QUERIES` | `100` | ANN latency samples |
| `PERF_RECALL_QUERIES` | `20` | Queries used for recall@10 |
| `PERF_CONCURRENCY` | `8` | Scan partitions and ANN client concurrency |
| `PERF_MUTATION_BUCKET` | `7` | First `id % 100` bucket for conditional mutation |
| `PERF_MUTATION_WINDOWS` | `3` | Disjoint mutation windows; window `i` uses `bucket + 10 * i` |
| `PERF_LANCE_PREFIX` | `perf-eval/lance` | Prefix under the bucket for Lance datasets |

## Running

```bash
cargo build --release --bins

# Full pipeline: ingest, sizes, vector index, sizes, queries.
scripts/run_all.sh my-run

# Add IndexLake's transactional insert path for comparison.
RUN_NATIVE_INSERT=1 scripts/run_all.sh my-run

# Individual phases.
target/release/indexlake-bench ingest-bulk my-run
target/release/indexlake-bench ingest-insert my-run
target/release/indexlake-bench index my-run          # vector HNSW only
target/release/indexlake-bench query-scalar my-run   # full scan + range scan
target/release/indexlake-bench query-vector my-run
target/release/indexlake-bench mutate-conditional my-run

target/release/lance-bench ingest my-run
target/release/lance-bench index my-run              # vector HNSW only
target/release/lance-bench query-scalar my-run
target/release/lance-bench query-vector my-run
target/release/lance-bench mutate-conditional my-run
```

`run_all.sh` writes logs and size snapshots to `results/<run-id>/`.

Measuring a prefix manually:

```bash
python3 scripts/s3_size.py size <namespace-id>            # IndexLake
python3 scripts/s3_size.py size perf-eval/lance/<run-id>  # Lance
```

---

## Results

Measured on 2026-10-28 with the previous point-lookup suite. The current suite
measures a range scan instead; rerun `scripts/run_all.sh` to refresh the query
numbers below.

| Item | Value |
|---|---|
| CPU | 2 × Xeon Gold 6240, 72 vCPU |
| Memory | 251 GB |
| Object store | MinIO, local loopback HTTP |
| Catalog | PostgreSQL 18, local |
| Build | release |
| IndexLake | `4e3ff35` (`v0.5.0`, Arrow 58, `hnsw_rs`) |
| Lance | `5141551` (Arrow 58) |

Workload:

- Scalar: 1,000,000 rows (`id Int32`, 128-byte compressible text, 128-byte random binary), raw logical size 248.0 MiB.
- Vector: 100,000 × 1024-d `f32`, raw logical size 391.0 MiB.
- Point lookup (previous revision): equality on `id` with **no scalar index**.
- Vector index: HNSW L2, `ef_construction=400`, `ef_search=256`, `k=10`.

### Ingest (no indexes)

| Engine / path | Scalar 1M | Vector 100k |
|---|---:|---:|
| IndexLake `bypass_insert` | 3.09–3.53s, median **3.35s** (~299k rows/s) | 2.35–5.35s, median **4.65s** (~21.5k rows/s) |
| Lance `Dataset::write` | 1.60–1.84s, median **1.61s** (~620k rows/s) | 2.77–3.57s, median **3.26s** (~30.7k rows/s) |
| IndexLake native `insert` | 10.29s (API 3.55s + dump 6.74s) | 17.42s (API 8.68s + dump 8.74s) |

Paired-run ratios: Lance is ~2.1x faster on scalar and ~1.4x faster on vector.
MinIO write throughput varied significantly between windows; the paired ratios
are more stable than absolute times.

IndexLake's native `insert` still stages rows in PostgreSQL and dumps them
asynchronously, so it is 3x (scalar) to 4x (vector) slower than `bypass_insert`.

### Query

| Operation | IndexLake | Lance |
|---|---:|---:|
| Full scan, single stream | 1041.9 ms (960k rows/s) | 62.5 ms (16.0M rows/s) |
| Full scan, 8 streams | 364.4 ms (2.74M rows/s) | — |
| Point lookup (previous revision), no index, p50 / p95 | 418.5 / 465.9 ms | 7.52 / 10.96 ms |
| ANN p50 / p95 | 939.0 / 980.1 ms | 9.65 / 10.29 ms |
| ANN recall@10 (`ef=256`) | 0.820 | 0.345 |
| ANN QPS, client concurrency 8 | 5.5 | 814.1 |

In that revision, point queries were filter-only on both sides. Lance used
file/row-group statistics and cached metadata to prune to one row group, so a
no-index point lookup was still ~7.5 ms. IndexLake performed a catalog round
trip and re-opened Parquet metadata/row groups per query; without a BTree it
dropped from the previous indexed-path ~1.65 s to ~0.42 s, but remained ~56x
slower than Lance.

For ANN, IndexLake's pinned `hnsw_rs` implementation reaches higher recall but
is ~97x slower per query. Its latency is dominated by re-reading and
reconstructing the ~425 MB index from S3 on every query
(`indexlake/src/table/search.rs`); Lance loads the index once and caches it.
Read ANN latency together with recall: at `ef=256` the two implementations are
not at the same operating point.

### Conditional mutation

Measured on 2026-10-28 in a separate mutation-only run
(`mut-20261028T090658Z`) with the current revision. Each engine ingested two
dedicated 1M-row tables, then applied three disjoint `bucket = id % 100`
windows; every window affected exactly 10,000 rows (1%) and passed row-level
verification. Times are per window in milliseconds.

| Engine / operation | Windows (ms) | Median (ms) | Rows/s at median |
|---|---:|---:|---:|
| IndexLake conditional update | 574 / 650 / 659 | 650 | 15,385 |
| IndexLake conditional delete | 429 / 416 / 373 | 416 | 24,038 |
| Lance conditional update | 205 / 225 / 125 | 205 | 48,780 |
| Lance conditional delete | 86 / 34 / 97 | 86 | 116,279 |

Post-mutation storage state:

| Engine / table | S3 footprint | State |
|---|---:|---|
| IndexLake update | 275.4 MiB (10 objects) | 30,000 rows in PostgreSQL inline storage; data files unchanged |
| IndexLake delete | 275.4 MiB (10 objects) | row validity updated in the catalog; data files unchanged |
| Lance update | 158.1 MiB (51 objects) | 13 fragments: 10 originals plus 3 rewritten row sets |
| Lance delete | 154.2 MiB (48 objects) | 10 fragments carrying deletion vectors |

Lance is ~3.2x faster on update and ~4.8x on delete by median, but the spread
across three windows is wide; read the per-window values too.

Caveats:

- IndexLake update timings are not end-to-end: the 30,000 updated rows stay in
  PostgreSQL inline storage and are not dumped/merged within the measurement.
  Lance writes the matched rows as new S3 fragments immediately.
- Windows run sequentially on one table, so later windows include the state
  left by earlier ones.
- No secondary index is built on the mutation tables. Mutation latency includes
  predicate evaluation and commit, and excludes the verification queries.

### Storage and compression

`stored/raw < 1` means compression.

| Dataset / engine | Data files | With index | Raw | Data/raw | Total/raw |
|---|---:|---:|---:|---:|---:|
| IndexLake scalar | 274.5 MiB | 274.5 MiB (no index) | 248.0 MiB | 1.107 | 1.107 |
| Lance scalar | 153.2 MiB | 153.2 MiB (no index) | 248.0 MiB | 0.618 | 0.618 |
| IndexLake vector | 399.3 MiB | 804.3 MiB | 391.0 MiB | 1.021 | 2.057 |
| Lance vector | 390.8 MiB | 795.3 MiB | 391.0 MiB | 0.999 | 2.034 |

Combined (no scalar index, vector HNSW included):

| Engine | Total | Raw | Total/raw |
|---|---:|---:|---:|
| IndexLake | 1078.8 MiB | 639.0 MiB | 1.688 |
| Lance | 948.5 MiB | 639.0 MiB | 1.484 |

IndexLake's Parquet writer is uncompressed, so its scalar data is 1.107x the
logical input while Lance stores it at 0.618x. Vector data is incompressible in
both; the HNSW index stores a full copy of the vectors, so both vector tables
approximately double. The HNSW index itself is nearly identical in size:
IndexLake 404.9 MiB, Lance 404.5 MiB.

### Index build

| Index | IndexLake | Lance |
|---|---:|---:|
| HNSW, 100k × 1024, `efConstruction=400` | 69.1 min | 41.3 s |

Lance builds with many threads; IndexLake's pinned `hnsw_rs` builder is
single-threaded per data file and this dataset is a single 100k-row file. No
BTree is built.

## Methodology Notes

- **Bulk ingest** uses IndexLake's `bypass_insert`, which streams Parquet data
  files and commits once, bypassing PostgreSQL inline staging. This is the fair
  counterpart to Lance's `Dataset::write`. `ingest-insert` measures the
  transactional path separately.
- **Range scan does not use a secondary index.** No BTree is created; the
  predicate is `id >= start AND id < start + PERF_RANGE_ROWS`, letting each
  engine use only its scan/pruning path. Random starts usually cross a row-group
  boundary, so a default 10k-row scan reads one or two row groups. Only `id` is
  projected.
- **No index maintenance** is included in ingest: tables are created without
  indexes, and the vector index is built in a separate phase.
- **Queries are warm-cache**: warmups are excluded, then p50/p95 are reported.
  The full-scan phase reports all three runs and the best one.
- **Compression** is `stored bytes / logical raw bytes`; values below 1 mean
  compression. Raw scalar bytes include a 4-byte id, 128-byte text, and
  128-byte binary per row. Raw vector bytes include a 4-byte id and 1024 × 4
  bytes per vector.
- **ANN ground truth** is an exact squared-L2 scan over the same generated
  vectors. Report latency together with recall.
- **Conditional mutation** uses dedicated tables created by the
  `mutate-conditional` phase; the query datasets are never modified. The scalar
  schema adds `bucket Int32 = id % 100`, so a `bucket = N` predicate cannot be
  pruned by Parquet/Lance min/max statistics. Every window targets a different
  bucket (`7, 17, 27` by default), reports the affected row count, and verifies
  both target and non-target rows. Mutation latency includes predicate
  evaluation and commit; it excludes verification queries. Windows run
  sequentially on the same table, so later windows include the state left by
  earlier ones; the log reports every window instead of only a median.
- **Where updates land matters.** IndexLake writes updated rows to PostgreSQL
  inline storage and invalidates the old rows; Lance writes the matched rows as
  new fragments and adds deletion vectors. IndexLake's deferred inline-row
  dump/merge is not included in the update latency, so compare timings and
  storage locations together, not S3 bytes alone.

### Fairness caveats

- MinIO is a local shared instance. Disk and network contention dominated
  ingest variance; use paired runs and medians.
- The vector dataset is uniform random, which is intentionally hard for ANN.
  Absolute recall is not representative of clustered real-world data.
- IndexLake and Lance use different HNSW implementations and graph parameters.
  The pinned IndexLake revision predates both the Arrow 59 upgrade and the
  in-house HNSW rewrite.
- IndexLake query latency includes PostgreSQL catalog round trips. Lance keeps
  metadata and indexes in memory through its caches.
- Both engines now resolve to a single Arrow 58 in `Cargo.lock`.

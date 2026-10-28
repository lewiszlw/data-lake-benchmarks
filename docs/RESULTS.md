# Measured Results

Measurements taken on 2026-10-28.

| Component | Revision |
|---|---|
| IndexLake | `0c662b3` / `6fe5c37` (the HNSW and table code is identical between these revisions) |
| Lance | `5141551` |

Hardware and services:

| Item | Value |
|---|---|
| CPU | 2 × Xeon Gold 6240, 72 vCPU |
| Memory | 251 GB |
| Object store | MinIO, local loopback HTTP |
| Catalog | PostgreSQL 18, local |
| Build | release |

Workload:

- Scalar: 1,000,000 rows (`id Int32`, 128-byte compressible text, 128-byte random binary), raw logical size 248.0 MiB.
- Vector: 100,000 × 1024-d `f32`, raw logical size 391.0 MiB.
- BTree on `id`; HNSW L2, `ef_construction=400`, `ef_search=256`, `k=10`.
- IndexLake HNSW uses the in-house graph (`M0=32`); Lance uses `IVF_HNSW_FLAT` with one partition and `M=16`.

## Ingest (no indexes)

| Engine / path | Scalar 1M | Vector 100k |
|---|---:|---:|
| IndexLake `bypass_insert` | 2.71–3.12s (median 2.87s, ~349k rows/s) | 2.05–2.28s (median 2.18s, ~46k rows/s) |
| Lance `Dataset::write` | 1.44–1.56s (median 1.50s, ~666k rows/s) | 0.86–1.75s (typical 1.75s, ~57k rows/s) |
| IndexLake native `insert` | 9.62s (API 3.53s + dump 6.10s) | 15.99s (API 8.83s + dump 7.16s) |

Paired-run ratios (same time window): Lance is ~1.9x faster on scalar and
~1.3–1.5x faster on vector. Scalar storage is the main reason: Lance writes
160.6 MB, IndexLake writes 287.4 MB because its Parquet writer is currently
uncompressed. Physical write throughput is similar (~100 vs ~107 MB/s).

## Query

| Operation | IndexLake | Lance |
|---|---:|---:|
| Full scan, single stream | 1088.8 ms (918k rows/s) | 65.5 ms (15.27M rows/s) |
| Full scan, 8 streams | 282.6 ms (3.54M rows/s) | — |
| Point lookup p50 / p95 | 1657.0 / 1734.9 ms | 6.44 / 7.28 ms |
| ANN p50 / p95 | 981.7 / 1032.2 ms | 8.87 / 10.25 ms |
| ANN recall@10 (`ef=256`) | 0.45 | 0.37 |
| ANN QPS, client concurrency 8 | 4.6 | 841.4 |

### Why IndexLake point lookups are slow

IndexLake's file-backed indexes are opened, read, and rebuilt on every query:

- `indexlake/src/table/scan.rs`: `storage.open(...)` → `index_builder.read_file(...)` → `index_builder.build()` for each data file.
- `indexlake/src/table/search.rs`: the same pattern for HNSW index files.

With ten 100k-row data files, a point lookup decodes ten BTree index files
(~21.6 MB) per query. The vector query decodes the full ~425 MB HNSW index per
query. A control query on the non-indexed `payload` column, which uses Parquet
row-group statistics, runs in 75.8 ms p50 — faster than the indexed path.

Lance loads indexes once through its dataset/index caches and answers from
memory.

## Storage and compression

`stored/raw < 1` means compression.

| Dataset / engine | Data files | With index | Raw | Data/raw | Total/raw |
|---|---:|---:|---:|---:|---:|
| IndexLake scalar | 274.1 MiB | 294.7 MiB | 248.0 MiB | 1.105 | 1.188 |
| Lance scalar | 153.2 MiB | 159.4 MiB | 248.0 MiB | 0.618 | 0.643 |
| IndexLake vector | 399.3 MiB | 804.6 MiB | 391.0 MiB | 1.021 | 2.058 |
| Lance vector | 390.8 MiB | 795.3 MiB | 391.0 MiB | 0.999 | 2.034 |

Combined:

| Engine | Total with indexes | Raw | Total/raw |
|---|---:|---:|---:|
| IndexLake | 1099.3 MiB | 639.0 MiB | 1.720 |
| Lance | 954.7 MiB | 639.0 MiB | 1.494 |

The scalar gap is compression: Lance stores the text column at 61.8% of its raw
size; IndexLake's Parquet writer uses `UNCOMPRESSED`. Vector data is
incompressible in both, and an HNSW index stores a full copy of the vectors, so
both vector tables roughly double in size.

## Index build

| Index | IndexLake | Lance |
|---|---:|---:|
| BTree, 1M rows | 650 ms | 165 ms |
| HNSW, 100k × 1024, `efConstruction=400` | 409.0 s | 36.2 s |

For reference, IndexLake main `9305b14` with the previous `hnsw_rs`
implementation took 4340.9 s (72.3 min) for the same HNSW build and reached
recall 0.82 at `ef=256`. The in-house rewrite is ~10.6x faster to build but
measured recall dropped to 0.45 at the same beam width. Lance builds with many
threads; IndexLake builds one data file's index at a time.

## Caveats

- MinIO was a shared local instance; write throughput varied by more than 2x
  between windows. The ingest table uses medians from low-contention windows,
  and the paired ratios are the more reliable relative signal.
- Uniform random 1024-d vectors are deliberately hard for ANN. Recall numbers
  are useful for comparing implementations on identical data, not as a
  statement about production recall.
- IndexLake and Lance do not use identical graph parameters or the same number
  of index segments, so ANN latency should be read together with recall.
- IndexLake query latency includes PostgreSQL catalog round trips; Lance
  metadata is cached in the dataset.

# 数据湖基准测试

可复现的、基于 S3 的 [IndexLake](https://github.com/systemxlabs/indexlake)、
[Lance](https://github.com/lance-format/lance)、DataFusion ListingTable 与
iceberg-rust 对比。

两个引擎都直接从 git 引入，并固定在使用 **Arrow 58** 的 revision 上，因此
lockfile 只解析出一个 Arrow 版本。

| 操作 | 工作负载 |
|---|---|
| 批量写入 | 100 万行 scalar 与 10 万 × 1024 维向量，无索引 |
| 单条写入 | 向专用种子表逐条追加，无索引 |
| 全表扫描 | 读取全部 scalar 列并统计行数 |
| 范围扫描 | 在 100 万行上扫描连续的 `id ∈ [start, start + 10k)`，**无二级索引** |
| 向量检索 | 10 万条向量上的 Top-10 ANN（HNSW，L2） |
| 条件更新 | `bucket = id % 100` 命中 1% 的 scalar 行；把 `payload` 更新为固定 128 字节值 |
| 条件删除 | `bucket = id % 100` 命中 1% 的 scalar 行 |

DataFusion ListingTable 作为第三个基线，只参与批量写入、全表扫描和 range
scan；不参与向量检索和条件变更。

iceberg-rust 参与批量写入、单条写入、全表扫描和 range scan。iceberg-rust
0.10.1 没有公开的行级 update/delete 事务 API，因此不参与条件变更和索引查询。

存储字节数与压缩比通过 S3 对象大小测量。索引构建时间单独测量，避免污染写入与
查询数字。结果见下文。

## 依赖

| 引擎 | Revision | Arrow |
|---|---|---|
| IndexLake | `4e3ff351ac4c5a05ce4d469b91f48bf883d75e87` (`v0.5.0`) | 58 |
| Lance | `51415518c7566c938ba14fe29bcb357b8adf2123` | 58 |
| DataFusion | `54.1.0`（ListingTable） | 58 |
| Iceberg | `0.10.1`（`iceberg-storage-opendal` 0.10.1） | 58 |

版本固定写在 `Cargo.toml` 中。更新版本时请同步更新下文的结果。

## 前置条件

- Rust 1.89 或更高版本。
- Python 3.8 或更高版本（`scripts/s3_size.py` 只使用标准库）。
- 一个兼容 S3 的对象存储。默认指向本地 MinIO：
  `http://127.0.0.1:9000`，bucket 为 `indexlake`。
- PostgreSQL 用于 IndexLake 的 catalog。使用固定 IndexLake revision 对应的
  schema，已复制到 `scripts/init-indexlake-catalog.sql`。较新的 IndexLake
  revision 修改了 inline-index schema；用它们的 SQL 配当前 pin 可能导致向量
  检索失败。
- MinIO endpoint 必须绕过 `HTTP_PROXY`。`run_all.sh` 会把
  `127.0.0.1,localhost` 追加到 `NO_PROXY`。

本地服务示例：

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=password \
  -e MINIO_DEFAULT_BUCKETS=indexlake minio/minio server /data --console-address :9001

docker run -d --name pg -p 5432:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password postgres
psql -h 127.0.0.1 -U postgres -d postgres -f scripts/init-indexlake-catalog.sql
```

## 配置

所有配置都有本地默认值，可通过环境变量覆盖。

| 变量 | 默认值 | 用途 |
|---|---|---|
| `S3_ENDPOINT` | `http://127.0.0.1:9000` | 对象存储 endpoint |
| `S3_BUCKET` | `indexlake` | Bucket |
| `S3_ACCESS_KEY` | `admin` | Access key |
| `S3_SECRET_KEY` | `password` | Secret key |
| `S3_REGION` | `us-east-1` | Region |
| `PGHOST` | `localhost` | IndexLake catalog 主机 |
| `PGPORT` | `5432` | Catalog 端口 |
| `PGUSER` | `postgres` | Catalog 用户 |
| `PGPASSWORD` | `password` | Catalog 密码 |
| `PGDATABASE` | `postgres` | Catalog 数据库 |
| `PERF_SCALAR_ROWS` | `1000000` | Scalar 行数 |
| `PERF_SCALAR_BATCH` | `10000` | 每个 batch 的 scalar 行数 |
| `PERF_INLINE_LIMIT` | `100000` | IndexLake inline 行上限 / 每个 data file 的目标行数 |
| `PERF_ROW_GROUP` | `10000` | 每个 Parquet/Lance row group 的行数 |
| `PERF_VECTOR_ROWS` | `100000` | 向量行数 |
| `PERF_VECTOR_BATCH` | `1000` | 每个 batch 的向量数 |
| `PERF_DIM` | `1024` | 向量维度 |
| `PERF_K` | `10` | ANN 返回条数 |
| `PERF_EF_SEARCH` | `256` | HNSW 搜索 beam |
| `PERF_EF_CONSTRUCTION` | `400` | HNSW 构建 beam |
| `PERF_RANGE_QUERIES` | `200` | 范围扫描延迟采样数 |
| `PERF_RANGE_ROWS` | `10000` | 每次范围扫描返回的行数 |
| `PERF_ANN_QUERIES` | `100` | ANN 延迟采样数 |
| `PERF_RECALL_QUERIES` | `20` | 用于 recall@10 的查询数 |
| `PERF_CONCURRENCY` | `8` | 扫描分区数与 ANN 客户端并发度 |
| `PERF_MUTATION_BUCKET` | `7` | 条件变更的起始 `id % 100` bucket |
| `PERF_MUTATION_WINDOWS` | `3` | 互不重叠的变更窗口；窗口 `i` 使用 `bucket + 10 * i` |
| `PERF_SINGLE_INSERTS` | `100` | 单条写入的测量次数（另有 5 次 warmup） |
| `PERF_LANCE_PREFIX` | `perf-eval/lance` | Lance dataset 在 bucket 下的前缀 |
| `PERF_PARQUET_PREFIX` | `perf-eval/parquet` | DataFusion ListingTable 的 Parquet dataset 前缀 |
| `PERF_ICEBERG_PREFIX` | `perf-eval/iceberg` | iceberg-rust warehouse 前缀 |

## 运行

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

target/release/datafusion-bench ingest my-run          # Parquet dataset for ListingTable
target/release/datafusion-bench query-scalar my-run    # full scan + range scan

target/release/iceberg-bench ingest my-run             # bulk + single-row writes
target/release/iceberg-bench query-scalar my-run       # full scan + range scan
```

`ingest-bulk` / `ingest` 内部同时执行批量写入和单条写入基准。

`run_all.sh` 会把日志和大小快照写入 `results/<run-id>/`。

手动测量某个前缀：

```bash
python3 scripts/s3_size.py size <namespace-id>            # IndexLake
python3 scripts/s3_size.py size perf-eval/lance/<run-id>  # Lance
```

---

## 结果

以下结果来自多次独立运行，run id 在各小节标注。查询分为扫描和索引查询；扫描
包括全表扫描（单流 / 8 流）和 range scan。写入主表中的 IndexLake/Lance 数字
以及 ANN、存储主表仍是 2026-10-28 的历史测量；DataFusion、Iceberg 和单条写入
来自后续运行。

| 项目 | 值 |
|---|---|
| CPU | 2 × Xeon Gold 6240，72 vCPU |
| 内存 | 251 GB |
| 对象存储 | MinIO，本地 loopback HTTP |
| Catalog | PostgreSQL 18，本地 |
| 构建 | release |
| IndexLake | `4e3ff35`（`v0.5.0`，Arrow 58，`hnsw_rs`） |
| Lance | `5141551`（Arrow 58） |

工作负载：

- Scalar：1,000,000 行（`id Int32`、128 字节可压缩文本、128 字节随机二进制），
  逻辑原始大小 248.0 MiB。
- Vector：100,000 × 1024 维 `f32`，逻辑原始大小 391.0 MiB。
- 向量索引：HNSW L2，`ef_construction=400`，`ef_search=256`，`k=10`。

### 写入（无索引）

#### 批量写入

| 引擎 / 路径 | Scalar 100 万 | Vector 10 万 |
|---|---:|---:|
| IndexLake `bypass_insert` | 3.09–3.53s，中位数 **3.35s**（约 299k rows/s） | 2.35–5.35s，中位数 **4.65s**（约 21.5k rows/s） |
| Lance `Dataset::write` | 1.60–1.84s，中位数 **1.61s**（约 620k rows/s） | 2.77–3.57s，中位数 **3.26s**（约 30.7k rows/s） |
| DataFusion 批量写入（Parquet writer） | 3.21s（约 311k rows/s） | — |
| Iceberg FastAppend | 2.65s（约 377k rows/s） | — |

DataFusion 行来自 `df-20261103T034709Z` 的单次运行：使用 arrow-rs Parquet
writer、Zstd(3)，10 个文件，数据集大小 132.4 MiB。它不是 DataFusion SQL
`COPY` 路径，因为 ListingTable 本身是只读接口。

Iceberg 行来自 `ice-20261103T133414Z`：10 个 Zstd(3) data file 加一次
FastAppend 提交；scalar warehouse 大小 132.4 MiB。

成对运行比值：Lance 在 scalar 上约快 2.1x，在 vector 上约快 1.4x。MinIO
写入吞吐在不同时间窗口波动明显；成对比值比绝对时间更稳定。

#### 单条写入

单条写入现在作为 ingest phase 的一部分执行。IndexLake/Lance 数据来自
`ing-20261028T092057Z`，Iceberg 来自 `ice-20261103T133414Z`。专用种子表先
通过 bulk 路径写入 100 万行，再逐条追加 100 条（5 次 warmup 不计入）。时间
为每次写入。

| 引擎 | p50 (ms) | p95 (ms) | mean (ms) | min / max (ms) | p50 对应 inserts/s |
|---|---:|---:|---:|---:|---:|
| IndexLake | 1.304 | 1.643 | 1.361 | 0.958 / 4.291 | 767 |
| Lance | 13.582 | 18.018 | 13.311 | 4.581 / 18.686 | 74 |
| Iceberg | 26.834 | 33.793 | 27.364 | 14.418 / 40.925 | 37 |

写入后状态：

| 引擎 | S3 占用 | 状态 |
|---|---:|---|
| IndexLake | 274.5 MiB（10 objects） | 105 行在 PostgreSQL inline storage；data files 未变 |
| Lance | 153.9 MiB（327 objects） | 115 个 fragment（10 个种子 + 105 次单条 append） |
| Iceberg | 138.4 MiB（434 objects） | 106 个 snapshot（1 次 seed + 105 次单条 append） |

按 p50，Lance 单条写入比 IndexLake 慢约 10.4x；Iceberg 为 26.8 ms，约为
IndexLake 的 20x、Lance 的 2x。Iceberg 每条都写一个 data file 并提交一次
snapshot，成本主要在 metadata/manifest 提交。IndexLake 只提交一条 PostgreSQL
inline 行；其延迟 dump 未计入。

这是最坏的逐条提交模式。应用只要每次提交多行，就能摊薄 Lance 和 Iceberg
的提交成本。

### 查询

查询分为两块：**扫描**（无二级索引）和**索引查询**（HNSW）。

#### 扫描

| 操作 | IndexLake | Lance | DataFusion | Iceberg |
|---|---:|---:|---:|---:|
| 全表扫描，单流 | 1038.7 ms（963k rows/s） | 65.3 ms（15.3M rows/s） | 640.2 ms（1.56M rows/s） | 797.7 ms（1.25M rows/s） |
| 全表扫描，8 流 | 328.2 ms（3.05M rows/s） | — | 149.1 ms（6.71M rows/s） | 294.9 ms（3.39M rows/s） |
| Range scan，1%（1 万行），p50 / p95 | 294.424 / 321.707 ms | 7.561 / 12.344 ms | 6.390 / 9.027 ms | 9.114 / 11.989 ms |

IndexLake/Lance 的数字来自 `ing-20261028T092057Z`，DataFusion 来自
`df-20261103T034709Z`，Iceberg 来自 `ice-20261103T133414Z`。全表扫描取 3 次
运行的最佳值；IndexLake/Lance 的首次运行包含冷启动（分别为 3.66 s 和
5.58 s），因此没有计入。DataFusion 的 Parquet 数据集由
`datafusion-bench ingest` 写出（Zstd(3)，10 个文件，每个 row group 1 万行）；
单流使用 `target_partitions=1`，8 流使用 `target_partitions=8`，SQL 规划时间
计入延迟。Iceberg 的 8 流对应 8 个并发 data file。

Range scan 的 p50 差距主要来自元数据/规划路径：Lance、DataFusion 和 Iceberg
都在 6–9 ms，IndexLake 为 294 ms。

#### 索引查询

| 操作 | IndexLake | Lance |
|---|---:|---:|
| ANN p50 / p95 | 939.0 / 980.1 ms | 9.65 / 10.29 ms |
| ANN recall@10（`ef=256`） | 0.820 | 0.345 |
| ANN QPS，客户端并发 8 | 5.5 | 814.1 |

对于 ANN，固定版本的 IndexLake `hnsw_rs` 实现 recall 更高，但每次查询约慢
97x。其延迟主要来自每次查询都从 S3 重新读取并重建约 425 MB 的索引
（`indexlake/src/table/search.rs`）；Lance 只加载一次并缓存。阅读 ANN 延迟
时必须同时看 recall：在 `ef=256` 时两个实现并不处于同一工作点。

Iceberg 不参与索引查询；iceberg-rust 0.10.1 也没有行级 update/delete 事务
action，因此条件变更同样不适用。

### 条件变更

于 2026-10-28 使用当前 revision 在独立的 mutation-only run
（`mut-20261028T090658Z`）中测得。每个引擎先写入两张专用的 100 万行表，然后
执行 3 个互不重叠的 `bucket = id % 100` 窗口；每个窗口都精确命中 10,000 行
（1%），并通过行级校验。时间为每个窗口的毫秒数。

| 引擎 / 操作 | 各窗口 (ms) | 中位数 (ms) | 中位数吞吐 (rows/s) |
|---|---:|---:|---:|
| IndexLake 条件更新 | 574 / 650 / 659 | 650 | 15,385 |
| IndexLake 条件删除 | 429 / 416 / 373 | 416 | 24,038 |
| Lance 条件更新 | 205 / 225 / 125 | 205 | 48,780 |
| Lance 条件删除 | 86 / 34 / 97 | 86 | 116,279 |

变更后存储状态：

| 引擎 / 表 | S3 占用 | 状态 |
|---|---:|---|
| IndexLake update | 275.4 MiB（10 objects） | 30,000 行在 PostgreSQL inline storage；data files 未变 |
| IndexLake delete | 275.4 MiB（10 objects） | 在 catalog 中更新行 validity；data files 未变 |
| Lance update | 158.1 MiB（51 objects） | 13 个 fragment：10 个原始 + 3 个重写行集 |
| Lance delete | 154.2 MiB（48 objects） | 10 个 fragment 带 deletion vector |

按中位数，Lance update 约快 3.2x，delete 约快 4.8x，但 3 个窗口的波动较大；
请同时看逐窗口数字。

注意事项：

- IndexLake update 不是端到端时间：30,000 行更新留在 PostgreSQL inline
  storage，测量期间没有 dump/merge。Lance 则立即把命中行写成新的 S3 fragment。
- 窗口在同一张表上顺序执行，后一个窗口包含前一个窗口留下的状态。
- 变更表不建二级索引。变更延迟包含谓词求值与提交，不包含校验查询。

### 存储与压缩

`stored/raw < 1` 表示有压缩。

| 数据集 / 引擎 | Data files | 含索引 | Raw | Data/raw | Total/raw |
|---|---:|---:|---:|---:|---:|
| IndexLake scalar | 274.5 MiB | 274.5 MiB（无索引） | 248.0 MiB | 1.107 | 1.107 |
| Lance scalar | 153.2 MiB | 153.2 MiB（无索引） | 248.0 MiB | 0.618 | 0.618 |
| IndexLake vector | 399.3 MiB | 804.3 MiB | 391.0 MiB | 1.021 | 2.057 |
| Lance vector | 390.8 MiB | 795.3 MiB | 391.0 MiB | 0.999 | 2.034 |

合计（无 scalar 索引，含向量 HNSW）：

| 引擎 | Total | Raw | Total/raw |
|---|---:|---:|---:|
| IndexLake | 1078.8 MiB | 639.0 MiB | 1.688 |
| Lance | 948.5 MiB | 639.0 MiB | 1.484 |

IndexLake 的 Parquet writer 不压缩，因此 scalar 数据是逻辑输入的 1.107x；
Lance 为 0.618x。向量数据两边都不可压缩；HNSW 索引保存向量的完整副本，因此
两个向量表大约翻倍。HNSW 索引本身大小几乎相同：IndexLake 404.9 MiB，
Lance 404.5 MiB。

### 索引构建

| 索引 | IndexLake | Lance |
|---|---:|---:|
| HNSW，10 万 × 1024，`efConstruction=400` | 69.1 min | 41.3 s |

Lance 使用多线程构建；固定版本的 IndexLake `hnsw_rs` builder 对每个 data file
单线程，而这个 dataset 是单个 10 万行文件。未构建 BTree。

## 方法论说明

- **批量写入** 使用 IndexLake 的 `bypass_insert`，它流式写 Parquet data file
  并只提交一次，绕过 PostgreSQL inline staging。这是 Lance `Dataset::write`
  的公平对照。`ingest-insert` 单独测量事务路径。
- **扫描类查询不使用二级索引。** 全表扫描和 range scan 都不创建 BTree；
  range scan 的谓词是 `id >= start AND id < start + PERF_RANGE_ROWS`，各引擎
  只能使用自己的 scan/pruning 路径。随机起点通常会跨 row-group 边界，因此
  默认 10k 行扫描会读取 1–2 个 row group。只投影 `id`。索引查询则是 HNSW。
- **写入不包含索引维护**：建表时不建索引，向量索引在单独 phase 构建。
- **查询是热缓存**：排除 warmup，然后报告 p50/p95。全表扫描 phase 报告 3 次
  运行并取最好的一次。
- **压缩比** 为 `stored bytes / logical raw bytes`；小于 1 表示有压缩。scalar
  原始字节包含每行 4 字节 id、128 字节文本和 128 字节二进制。向量原始字节
  包含每行 4 字节 id 与 1024 × 4 字节。
- **ANN ground truth** 是对同一批生成向量做精确平方 L2 扫描。延迟必须与
  recall 一起看。
- **条件变更** 使用 `mutate-conditional` phase 创建的专用表，不会修改查询
  dataset。scalar schema 增加 `bucket Int32 = id % 100`，因此
  `bucket = N` 谓词无法被 Parquet/Lance 的 min/max 统计剪枝。每个窗口命中
  不同的 bucket（默认 `7, 17, 27`），报告命中行数，并校验目标行与非目标行。
  变更延迟包含谓词求值与提交，不包含校验查询。窗口在同一张表上顺序执行，
  后一个窗口包含前一个窗口留下的状态；日志逐窗口上报，而不是只给中位数。
- **更新数据落在哪里很重要。** IndexLake 把更新行写入 PostgreSQL inline
  storage，并把旧行置为 invalid；Lance 把命中行写成新 fragment，并添加
  deletion vector。IndexLake 延迟的 inline-row dump/merge 不计入 update
  延迟，因此要结合时间和存储位置一起看，不能只看 S3 字节数。
- **单条写入** 作为 ingest phase 的一部分执行：先用 bulk 路径写入一张专用
  100 万行种子表，再逐条追加；5 次 warmup 不计入。IndexLake 把每条暂存在
  PostgreSQL inline storage，测量窗口低于 `PERF_INLINE_LIMIT`，因此不会触发
  dump，延迟物化未计入。Lance 每次 append 提交一个新 fragment/version，因此
  延迟和 fragment 数包含逐条 S3 写入与提交。
- **DataFusion ListingTable 基线** 读取由 `datafusion-bench ingest` 写出的
  Parquet dataset。写入使用 arrow-rs Parquet writer，Zstd(3)，每 10 万行一个
  文件、每 1 万行一个 row group。查询通过 `SessionContext` 注册
  ListingTable：单流 `target_partitions=1`，8 流 `target_partitions=8`；
  SQL 规划时间计入延迟。ListingTable 本身是只读接口，因此批量写入指标衡量
  的是生成该 Parquet dataset 的写入路径，而不是 DataFusion SQL `COPY`。
  三者使用各自的文件布局和压缩，扫描对比不是纯执行引擎差异。
- **iceberg-rust 基线** 使用 `iceberg 0.10.1` + `iceberg-storage-opendal`。
  MemoryCatalog 仅用于 ingest 进程内的表注册，metadata/data 都写在 S3。写入
  使用 Zstd(3)、10 万行/文件、1 万行/row group；批量写入为 10 个 data file
  加一次 FastAppend 提交，单条写入为逐条 data file 加独立提交。查询进程通过
  ingest 写下的 metadata marker 直接加载 Table，不依赖 MemoryCatalog 跨进程
  持久化。全表扫描分别用 1 和 8 个并发 data file；range scan 使用 Iceberg
  谓词与 row-group filtering。iceberg-rust 0.10.1 没有行级 update/delete
  事务 action，因此不测这两项。

### 公平性说明

- MinIO 是本地共享实例。磁盘与网络竞争主导了写入波动；请使用成对运行和
  中位数。
- 向量 dataset 是均匀随机数据，对 ANN 故意很难。绝对 recall 不代表真实聚类
  数据。
- IndexLake 与 Lance 使用不同的 HNSW 实现和图参数。固定的 IndexLake revision
  早于 Arrow 59 升级和自研 HNSW 重写。
- IndexLake 查询延迟包含 PostgreSQL catalog 往返。Lance 通过缓存把元数据和
  索引保存在内存中。
- 两个引擎现在在 `Cargo.lock` 中都解析到单一的 Arrow 58。

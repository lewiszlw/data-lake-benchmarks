#!/usr/bin/env bash
# End-to-end run: ingest both engines without indexes (bulk plus single-row
# inserts), measure storage, build indexes, measure storage again, then run the
# query phases.
#
# Usage: scripts/run_all.sh [run-id]
# Environment: see README.md. The MinIO endpoint should stay out of HTTP_PROXY.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="$ROOT/results/$RUN_ID"
mkdir -p "$OUT"

# Local MinIO must bypass any corporate HTTP proxy.
export NO_PROXY="${NO_PROXY:+${NO_PROXY},}127.0.0.1,localhost"
export no_proxy="${no_proxy:+${no_proxy},}127.0.0.1,localhost"

: "${S3_BUCKET:=indexlake}"
: "${PERF_LANCE_PREFIX:=perf-eval/lance}"
: "${PERF_PARQUET_PREFIX:=perf-eval/parquet}"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    cargo build --release --bins
fi

IL="$ROOT/target/release/indexlake-bench"
LA="$ROOT/target/release/lance-bench"
DF="$ROOT/target/release/datafusion-bench"
S3="$ROOT/scripts/s3_size.py"

echo "run-id: $RUN_ID"
echo "results: $OUT"

echo "== ingest (no indexes) =="
"$IL" ingest-bulk "$RUN_ID" | tee "$OUT/indexlake-ingest.log"
"$LA" ingest "$RUN_ID" | tee "$OUT/lance-ingest.log"
"$DF" ingest "$RUN_ID" | tee "$OUT/datafusion-ingest.log"

IL_SCALAR_PREFIX="$(sed -n 's/.*scalar_prefix=\([^ ]*\).*/\1/p' "$OUT/indexlake-ingest.log" | tail -1)"
IL_VECTOR_PREFIX="$(sed -n 's/.*vector_prefix=\([^ ]*\).*/\1/p' "$OUT/indexlake-ingest.log" | tail -1)"
IL_INSERT_PREFIX="$(sed -n 's/.*table=single_insert .*prefix=\([^ ]*\).*/\1/p' "$OUT/indexlake-ingest.log" | tail -1)"
LA_PREFIX="$S3_BUCKET/$PERF_LANCE_PREFIX/$RUN_ID"
DF_PREFIX="$PERF_PARQUET_PREFIX/$RUN_ID/scalar"
: "${IL_SCALAR_PREFIX:?failed to parse indexlake scalar prefix}"
: "${IL_VECTOR_PREFIX:?failed to parse indexlake vector prefix}"
: "${IL_INSERT_PREFIX:?failed to parse indexlake single-insert prefix}"

{
    echo "stage=after_ingest"
    echo -n "indexlake_scalar        "; python3 "$S3" size "$IL_SCALAR_PREFIX"
    echo -n "indexlake_vector        "; python3 "$S3" size "$IL_VECTOR_PREFIX"
    echo -n "indexlake_insert_single "; python3 "$S3" size "$IL_INSERT_PREFIX"
    echo -n "indexlake_total         "; python3 "$S3" size "${IL_SCALAR_PREFIX%/*}"
    echo -n "lance_scalar            "; python3 "$S3" size "$LA_PREFIX/scalar"
    echo -n "lance_vector            "; python3 "$S3" size "$LA_PREFIX/vector"
    echo -n "lance_insert_single     "; python3 "$S3" size "$LA_PREFIX/single_insert"
    echo -n "lance_total             "; python3 "$S3" size "$LA_PREFIX"
    echo -n "datafusion_scalar       "; python3 "$S3" size "$DF_PREFIX"
    echo -n "datafusion_total        "; python3 "$S3" size "$PERF_PARQUET_PREFIX/$RUN_ID"
} | tee "$OUT/sizes-after-ingest.txt"

echo "== build indexes =="
"$IL" index "$RUN_ID" | tee "$OUT/indexlake-index.log"
"$LA" index "$RUN_ID" | tee "$OUT/lance-index.log"

{
    echo "stage=after_index"
    echo -n "indexlake_scalar "; python3 "$S3" size "$IL_SCALAR_PREFIX"
    echo -n "indexlake_vector "; python3 "$S3" size "$IL_VECTOR_PREFIX"
    echo -n "indexlake_total  "; python3 "$S3" size "${IL_SCALAR_PREFIX%/*}"
    echo -n "lance_scalar    "; python3 "$S3" size "$LA_PREFIX/scalar"
    echo -n "lance_vector    "; python3 "$S3" size "$LA_PREFIX/vector"
    echo -n "lance_total     "; python3 "$S3" size "$LA_PREFIX"
} | tee "$OUT/sizes-after-index.txt"

if [[ "${RUN_NATIVE_INSERT:-0}" == "1" ]]; then
    echo "== IndexLake native insert path =="
    "$IL" ingest-insert "$RUN_ID" | tee "$OUT/indexlake-ingest-insert.log"
fi

echo "== queries =="
"$IL" query-scalar "$RUN_ID" | tee "$OUT/indexlake-query-scalar.log"
"$LA" query-scalar "$RUN_ID" | tee "$OUT/lance-query-scalar.log"
"$DF" query-scalar "$RUN_ID" | tee "$OUT/datafusion-query-scalar.log"
"$IL" query-vector "$RUN_ID" | tee "$OUT/indexlake-query-vector.log"
"$LA" query-vector "$RUN_ID" | tee "$OUT/lance-query-vector.log"

# Mutations run after the queries and use dedicated tables, so they never
# contaminate the query datasets above.
echo "== conditional mutations =="
"$IL" mutate-conditional "$RUN_ID" | tee "$OUT/indexlake-mutate-conditional.log"
"$LA" mutate-conditional "$RUN_ID" | tee "$OUT/lance-mutate-conditional.log"

IL_UPDATE_PREFIX="$(sed -n 's/.*table=scalar_update_cond .*prefix=\([^ ]*\).*/\1/p' "$OUT/indexlake-mutate-conditional.log" | tail -1)"
IL_DELETE_PREFIX="$(sed -n 's/.*table=scalar_delete_cond .*prefix=\([^ ]*\).*/\1/p' "$OUT/indexlake-mutate-conditional.log" | tail -1)"
: "${IL_UPDATE_PREFIX:?failed to parse indexlake conditional-update prefix}"
: "${IL_DELETE_PREFIX:?failed to parse indexlake conditional-delete prefix}"

{
    echo "stage=after_mutation"
    echo -n "indexlake_update_cond "; python3 "$S3" size "$IL_UPDATE_PREFIX"
    echo -n "indexlake_delete_cond "; python3 "$S3" size "$IL_DELETE_PREFIX"
    echo -n "lance_update_cond    "; python3 "$S3" size "$LA_PREFIX/scalar_update_cond"
    echo -n "lance_delete_cond    "; python3 "$S3" size "$LA_PREFIX/scalar_delete_cond"
} | tee "$OUT/sizes-after-mutation.txt"

echo "done: $OUT"

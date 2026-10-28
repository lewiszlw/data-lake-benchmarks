#!/usr/bin/env bash
# End-to-end run: ingest both engines without indexes, measure storage, build
# indexes, measure storage again, then run the query phases.
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

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    cargo build --release -p indexlake-bench -p lance-bench
fi

IL="$ROOT/target/release/indexlake-bench"
LA="$ROOT/target/release/lance-bench"
S3="$ROOT/scripts/s3_size.py"

echo "run-id: $RUN_ID"
echo "results: $OUT"

echo "== ingest (no indexes) =="
"$IL" ingest-bulk "$RUN_ID" | tee "$OUT/indexlake-ingest.log"
"$LA" ingest "$RUN_ID" | tee "$OUT/lance-ingest.log"

IL_SCALAR_PREFIX="$(sed -n 's/.*scalar_prefix=\([^ ]*\).*/\1/p' "$OUT/indexlake-ingest.log" | tail -1)"
IL_VECTOR_PREFIX="$(sed -n 's/.*vector_prefix=\([^ ]*\).*/\1/p' "$OUT/indexlake-ingest.log" | tail -1)"
LA_PREFIX="$S3_BUCKET/$PERF_LANCE_PREFIX/$RUN_ID"
: "${IL_SCALAR_PREFIX:?failed to parse indexlake scalar prefix}"
: "${IL_VECTOR_PREFIX:?failed to parse indexlake vector prefix}"

{
    echo "stage=after_ingest"
    echo -n "indexlake_scalar "; python3 "$S3" size "$IL_SCALAR_PREFIX"
    echo -n "indexlake_vector "; python3 "$S3" size "$IL_VECTOR_PREFIX"
    echo -n "indexlake_total  "; python3 "$S3" size "${IL_SCALAR_PREFIX%/*}"
    echo -n "lance_scalar    "; python3 "$S3" size "$LA_PREFIX/scalar"
    echo -n "lance_vector    "; python3 "$S3" size "$LA_PREFIX/vector"
    echo -n "lance_total     "; python3 "$S3" size "$LA_PREFIX"
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
"$IL" query-vector "$RUN_ID" | tee "$OUT/indexlake-query-vector.log"
"$LA" query-vector "$RUN_ID" | tee "$OUT/lance-query-vector.log"
"$IL" probe "$RUN_ID" | tee "$OUT/indexlake-probe.log"

echo "done: $OUT"

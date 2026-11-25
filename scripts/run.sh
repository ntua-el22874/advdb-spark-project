#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || ! "$1" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ || ! "$2" =~ ^[1-9][0-9]*$ ]]; then
    echo "Usage: bash scripts/run.sh LABEL REPEATS PYTHON_SCRIPT [SCRIPT_ARGS...]" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
label="$1"
repeats="$2"
script="$3"
shift 3
if [[ "$script" != /* ]]; then
    script="$repo_dir/$script"
fi
if [[ ! -f "$script" ]]; then
    echo "Python script not found: $script" >&2
    exit 2
fi

export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
submit=spark-submit
if [[ -n "${SPARK_HOME:-}" ]]; then
    submit="$SPARK_HOME/bin/spark-submit"
fi
command -v "$submit" >/dev/null || { echo "spark-submit not found. Set SPARK_HOME." >&2; exit 127; }
command -v "$PYSPARK_PYTHON" >/dev/null || { echo "Python not found: $PYSPARK_PYTHON" >&2; exit 127; }

results_dir="${ADVDB_RESULTS_DIR:-$repo_dir/results}"
mkdir -p "$results_dir/event-logs"
results_dir="$(cd "$results_dir" && pwd)"
event_log_uri="$("$PYSPARK_PYTHON" -c 'import sys; from pathlib import Path; print(Path(sys.argv[1]).resolve().as_uri())' "$results_dir/event-logs")"
batch_dir="$(mktemp -d "$results_dir/$label.XXXXXXXX")"
echo "Saving results to $batch_dir"

for ((index = 1; index <= repeats; index++)); do
    export ADVDB_RUN_DIR="$batch_dir/repeat-$index"
    mkdir "$ADVDB_RUN_DIR"
    echo "Submitting repetition $index of $repeats"
    if "$submit" \
        --master "${SPARK_MASTER:-spark://127.0.0.1:7077}" \
        --deploy-mode client \
        --driver-memory "${DRIVER_MEMORY:-2g}" \
        --executor-cores "${EXECUTOR_CORES:-1}" \
        --executor-memory "${EXECUTOR_MEMORY:-2g}" \
        --total-executor-cores "${TOTAL_EXECUTOR_CORES:-4}" \
        --conf "spark.dynamicAllocation.enabled=false" \
        --conf "spark.eventLog.enabled=true" \
        --conf "spark.eventLog.compress=true" \
        --conf "spark.eventLog.dir=$event_log_uri" \
        --py-files "$repo_dir/queries/config.py" \
        "$script" "$@" >"$ADVDB_RUN_DIR/stdout.log" 2>"$ADVDB_RUN_DIR/stderr.log"; then
        echo "Completed $ADVDB_RUN_DIR"
    else
        status=$?
        echo "spark-submit exited with status $status. See $ADVDB_RUN_DIR/stderr.log" >&2
        exit "$status"
    fi
done

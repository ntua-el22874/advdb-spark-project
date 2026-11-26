# Advanced Databases Spark project

## Start Spark on one host

Run these commands from the repository root in WSL2 or macOS. Reuse the Phase
1A Spark 4.2.x distribution with Java 17 or 21 and Python 3.10 or newer. Set
`SPARK_HOME`, `JAVA_HOME` and, if needed, `PYSPARK_PYTHON` in your shell to match
your installation.

```bash
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER=spark://127.0.0.1:7077
export ADVDB_DATA_DIR="${ADVDB_DATA_DIR:-$HOME/advdb}"
export ADVDB_RESULTS_DIR="${ADVDB_RESULTS_DIR:-$PWD/results}"
mkdir -p "$ADVDB_DATA_DIR/raw" "$ADVDB_DATA_DIR/parquet" "$ADVDB_RESULTS_DIR/event-logs"
event_log_uri="$("${PYSPARK_PYTHON:-python3}" -c 'import sys; from pathlib import Path; print(Path(sys.argv[1]).resolve().as_uri())' "$ADVDB_RESULTS_DIR/event-logs")"

env -u SPARK_MASTER "$SPARK_HOME/sbin/start-master.sh" --host 127.0.0.1 --port 7077
env -u SPARK_MASTER "$SPARK_HOME/sbin/start-worker.sh" --host 127.0.0.1 --cores 4 --memory 10g "$SPARK_MASTER"
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$event_log_uri" \
    env -u SPARK_MASTER "$SPARK_HOME/sbin/start-history-server.sh"
```

The Master UI at `http://127.0.0.1:8080` shows worker capacity. The four default
executors need at least 8 GiB of worker capacity in total. The 2 GiB client driver
and Spark daemons also need host memory. If Phase 1A services are running with a
smaller worker or a different history directory, stop the affected services
using their original daemon settings before restarting them. In particular,
reuse any custom `SPARK_PID_DIR` or `SPARK_IDENT_STRING` for the stop commands.
The daemon launcher also uses `SPARK_MASTER` as an rsync source, so the start
commands remove it from the daemon environment and pass the worker URL explicitly.

The History Server at `http://127.0.0.1:18080` reads the runner's compressed event
logs under `$ADVDB_RESULTS_DIR/event-logs`. Use the application ID to open a
completed run and inspect its jobs, stages, executors and environment. Restart
the History Server with the corresponding URI if you change the results root.
[Spark standalone mode](https://spark.apache.org/docs/4.2.0/spark-standalone.html) ·
[Spark monitoring](https://spark.apache.org/docs/4.2.0/monitoring.html)

## Submit a script

Put source files under `$ADVDB_DATA_DIR/raw`. Converters write to
`$ADVDB_DATA_DIR/parquet`, and the queries read from those same directories.

```bash
bash scripts/run.sh convert-reviews 1 queries/convert_reviews.py
bash scripts/run.sh q1-dataframe 1 queries/query1.py --api dataframe
```

The runner accepts `LABEL REPEATS PYTHON_SCRIPT [SCRIPT_ARGS...]` and starts one
`spark-submit` application per repetition. It uses `$SPARK_HOME/bin/spark-submit`
when `SPARK_HOME` is set and searches `PATH` otherwise. The Python default is
`python3`, which you can override with `PYSPARK_PYTHON`. The master defaults to
`spark://127.0.0.1:7077`. Override the resource defaults in your shell as needed.

| Variable | Default |
| --- | --- |
| `EXECUTOR_CORES` | `1` |
| `EXECUTOR_MEMORY` | `2g` |
| `TOTAL_EXECUTOR_CORES` | `4` |
| `DRIVER_MEMORY` | `2g` |

Each batch gets a unique directory under `ADVDB_RESULTS_DIR`, which defaults
to the repository's ignored `results/` directory. Inside it, each `repeat-N/`
contains `stdout.log` and `stderr.log` from `spark-submit`. Spark's Python
launcher forwards Python driver stderr to the submit process's stdout. The
runner exports that repetition's
absolute path as `ADVDB_RUN_DIR`. Query scripts can use
`Path(os.environ["ADVDB_RUN_DIR"])` to write results, timings and a formatted
plan captured from `df.explain(mode="formatted")`. Choose an outside-repository
directory or a path under `results/` when overriding the results root.

## Q0 benchmark

Stage the three Yelp JSON Lines files, `glove.6B.100d.txt`, and `acs_zcta.csv`
under `$ADVDB_DATA_DIR/raw`. Assignment page 3 gives 150,346 business records,
6,990,280 review records and 400,000 GloVe vectors. Check them with `wc -l`,
allowing the assignment's ±1 tolerance for a missing final newline.

```bash
wc -l "$ADVDB_DATA_DIR"/raw/yelp_academic_dataset_{business,review,user}.json \
    "$ADVDB_DATA_DIR"/raw/{glove.6B.100d.txt,acs_zcta.csv}

bash scripts/run.sh convert-reviews 1 queries/convert_reviews.py
bash scripts/run.sh convert-business-user 1 queries/convert_business_user.py
bash scripts/run.sh convert-acs 1 queries/convert_auxiliary.py acs
bash scripts/run.sh convert-glove 1 queries/convert_auxiliary.py glove
```

Verify the Parquet record counts against the inputs and check the schemas and
review year partitions before benchmarking. Compare allocated disk space in KiB
for the corresponding Yelp inputs and outputs.

```bash
du -sk "$ADVDB_DATA_DIR"/raw/yelp_academic_dataset_{business,review,user}.json \
    "$ADVDB_DATA_DIR"/parquet/{business,reviews,users}

for source in json-inferred json-explicit parquet; do
    bash scripts/run.sh "q0-$source" 3 queries/benchmark_q0.py "$source"
done
```

Keep the cluster settings the same for all three variants. Each application
writes a `*.rows.json`, a `*.plan.txt` and a `*.metrics.json` in its run
directory. Check that the year keys and average stars agree within floating-point
tolerance. Report the median and min–max of `action_seconds`, which times the
final `collect()`. Report `setup_seconds` separately to show the read and query
setup cost, including JSON schema inference. `total_seconds` is the sum of the
two. The metrics include the application ID and event-log directory. Use the
application's stages in the History Server to report task counts. The jobs are
tagged `q0-setup` and `q0-collect` in the event log.

## Q1A correctness checks

Q1 selects reviews from 2015 through 2021 with at least one useful vote.
Both parts exclude null text because its length is unknown. In Q1(b) the
denominator is the number of reviews in that year meeting the year and
useful-vote filters and having non-null text. An empty string counts as a
Short review of length zero. A category with no reviews in a given year
has no row for that year.

Use a temporary data root for the fixture. The normal review converter
writes its Parquet input for both APIs.

```bash
(
    export ADVDB_DATA_DIR="$(mktemp -d)"
    python3 scripts/verify_q1.py fixture "$ADVDB_DATA_DIR"
    bash scripts/run.sh q1-fixture-convert 1 queries/convert_reviews.py
    bash scripts/run.sh q1-fixture-dataframe 1 queries/query1.py --api dataframe --audit
    bash scripts/run.sh q1-fixture-sql 1 queries/query1.py --api sql
)

bash scripts/run.sh q1-full-dataframe 1 queries/query1.py --api dataframe --audit
bash scripts/run.sh q1-full-sql 1 queries/query1.py --api sql
```

Set the four directory variables below to the corresponding `repeat-1`
directories printed by the runner.

```bash
python3 scripts/verify_q1.py check "$fixture_dataframe_dir" "$fixture_sql_dir" --fixture
python3 scripts/verify_q1.py check "$full_dataframe_dir" "$full_sql_dir"
```

Each run writes `q1-a` and `q1-b` rows, formatted physical plans, and metrics
with the Spark application ID. With `--audit`, a separate action after
Q1(b) writes `q1-eligibility.rows.json` with yearly counts before the text
filter and the number of those reviews with null text. The verifier checks
the fixture against hand-calculated results, compares the APIs, and
reconciles category counts and yearly percentage sums with the audit.

## Q1B cache and timing

With the same standalone cluster and full review Parquet used for Q1A, run
three fresh applications per API and cache setting. The runner defaults give
four one-core executors with 2 GiB each and a 2 GiB driver.

```bash
export EXECUTOR_CORES=1 EXECUTOR_MEMORY=2g TOTAL_EXECUTOR_CORES=4 DRIVER_MEMORY=2g
for api in dataframe sql; do
    for cache in none filtered; do
        bash scripts/run.sh "q1-$api-$cache" 3 queries/query1.py --api "$api" --cache "$cache"
    done
done
bash scripts/run.sh q1-full-cache 1 queries/query1.py --api dataframe --cache full

du -sc -B1 "$ADVDB_DATA_DIR"/parquet/reviews/year={2015..2021}
```

Filtered mode caches the filtered projection of `year`, `stars`, `useful`
and `text` before computing categories. Q1(a) fills the cache as part of
its timed action. Q1(b) then reads from the same cache. Each action has its
own `action_seconds`, and `q1-sequence.metrics.json` records the sum.
Plan capture, file writes, Storage inspection and the optional eligibility
audit happen after both actions.

The full-cache application is a separate diagnostic. It caches the entire
input schema and materializes it with a review count and total text length.
In both cache modes, `storage.json` captures the live Spark Storage API
before unpersisting. Compare the filtered cache with the allocated size
of the original 2015–2021 Parquet directories, which contain all reviews
and columns in those years.

## Stop Spark

```bash
"$SPARK_HOME/sbin/stop-history-server.sh"
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
```

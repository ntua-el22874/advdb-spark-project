import argparse
import io
import json
import os
from contextlib import redirect_stdout
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import PARQUET_DIR, RAW_DIR


# Match the schema in convert_reviews.py.
REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("business_id", StringType(), False),
    StructField("stars", FloatType(), False),
    StructField("useful", IntegerType(), False),
    StructField("funny", IntegerType(), False),
    StructField("cool", IntegerType(), False),
    StructField("text", StringType(), True),
    StructField("date", TimestampType(), False),
])


def annual_average(spark, source):
    if source == "parquet":
        reviews = spark.read.parquet(str(PARQUET_DIR / "reviews"))
    else:
        reader = spark.read
        if source == "json-explicit":
            reader = reader.schema(REVIEW_SCHEMA)
        reviews = reader.json(str(RAW_DIR / "yelp_academic_dataset_review.json"))
        date = F.col("date")
        if source == "json-inferred":
            date = date.try_cast("timestamp_ltz")
        reviews = reviews.withColumn("year", F.year(date))

    return reviews.groupBy("year").agg(F.avg("stars").alias("avg_stars")).orderBy("year")


def write_artifacts(source, run_dir, query, rows, metrics):
    plan = io.StringIO()
    with redirect_stdout(plan):
        query.explain(mode="formatted")

    rows_json = json.dumps([row.asDict() for row in rows], indent=2) + "\n"
    metrics_json = json.dumps(metrics, indent=2) + "\n"
    if run_dir:
        directory = Path(run_dir)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{source}.plan.txt").write_text(plan.getvalue(), encoding="utf-8")
        (directory / f"{source}.rows.json").write_text(rows_json, encoding="utf-8")
        (directory / f"{source}.metrics.json").write_text(metrics_json, encoding="utf-8")
    else:
        print(plan.getvalue(), end="")
        print(rows_json, end="")
    print(metrics_json, end="")


def main():
    parser = argparse.ArgumentParser(description="Benchmark Q0 with one review input variant")
    parser.add_argument("source", choices=("json-inferred", "json-explicit", "parquet"))
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName(f"Q0: {args.source}")
        # Match the review converter when deriving the calendar year.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        context = spark.sparkContext
        context.setJobGroup("q0-setup", "Q0 read and query setup")
        started = perf_counter()
        query = annual_average(spark, args.source)
        setup_seconds = perf_counter() - started

        context.setJobGroup("q0-collect", "Q0 annual average collect")
        started = perf_counter()
        rows = query.collect()
        action_seconds = perf_counter() - started

        conf = context.getConf()
        metrics = {
            "source": args.source,
            "application_id": context.applicationId,
            "spark_version": spark.version,
            "master": context.master,
            "driver_memory": conf.get("spark.driver.memory"),
            "executor_cores": conf.get("spark.executor.cores"),
            "executor_memory": conf.get("spark.executor.memory"),
            "total_executor_cores": conf.get("spark.cores.max"),
            "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "event_log_dir": conf.get("spark.eventLog.dir") if conf.get("spark.eventLog.enabled") == "true" else None,
            "row_count": len(rows),
            "setup_seconds": setup_seconds,
            "action_seconds": action_seconds,
            "total_seconds": setup_seconds + action_seconds,
        }
        write_artifacts(args.source, os.environ.get("ADVDB_RUN_DIR"), query, rows, metrics)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

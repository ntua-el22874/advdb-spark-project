import argparse
import json
import os
from pathlib import Path
from time import perf_counter
from urllib.request import urlopen

from pyspark.sql import SparkSession, Window, functions as F

from benchmark_q0 import write_artifacts
from config import PARQUET_DIR


def eligible_reviews(reviews):
    return reviews.where(F.col("year").between(2015, 2021) & (F.col("useful") >= 1))


def filtered_reviews(spark, reviews, api):
    if api == "dataframe":
        return eligible_reviews(reviews).where(F.col("text").isNotNull()).select(
            "year", "stars", "useful", "text"
        )
    reviews.createOrReplaceTempView("q1_reviews")
    return spark.sql("""
        SELECT year, stars, useful, text
        FROM q1_reviews
        WHERE year BETWEEN 2015 AND 2021 AND useful >= 1 AND text IS NOT NULL
    """)


def dataframe_queries(filtered):
    length = F.length("text")
    categorized = filtered.withColumn(
        "category",
        F.when(length < 250, "Short")
        .when(length < 750, "Medium")
        .when(length < 2000, "Long")
        .otherwise("Very Long"),
    )
    part_a = categorized.groupBy("category").agg(
        F.count("*").alias("review_count"),
        F.avg("stars").alias("avg_stars"),
        F.avg("useful").alias("avg_useful"),
    ).orderBy(F.desc("avg_useful"), "category")

    counts = categorized.groupBy("year", "category").count()
    year_total = F.sum("count").over(Window.partitionBy("year"))
    part_b = counts.select(
        "year", "category", (100.0 * F.col("count") / year_total).alias("percentage")
    ).orderBy("year", "category")
    return part_a, part_b


def sql_queries(spark, filtered):
    filtered.createOrReplaceTempView("q1_filtered")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW q1_categorized AS
        SELECT year, stars, useful,
               CASE WHEN length(text) < 250 THEN 'Short'
                    WHEN length(text) < 750 THEN 'Medium'
                    WHEN length(text) < 2000 THEN 'Long'
                    ELSE 'Very Long' END AS category
        FROM q1_filtered
    """)
    part_a = spark.sql("""
        SELECT category, count(*) AS review_count, avg(stars) AS avg_stars,
               avg(useful) AS avg_useful
        FROM q1_categorized
        GROUP BY category
        ORDER BY avg_useful DESC, category
    """)
    part_b = spark.sql("""
        WITH counts AS (
            SELECT year, category, count(*) AS review_count
            FROM q1_categorized
            GROUP BY year, category
        )
        SELECT year, category,
               CAST(100 AS DOUBLE) * review_count
                   / sum(review_count) OVER (PARTITION BY year) AS percentage
        FROM counts
        ORDER BY year, category
    """)
    return part_a, part_b


def write_json(run_dir, filename, data):
    contents = json.dumps(data, indent=2) + "\n"
    if run_dir:
        (Path(run_dir) / filename).write_text(contents, encoding="utf-8")
    else:
        print(contents, end="")


def record_storage(context, cached, run_dir, metrics):
    url = f"{context.uiWebUrl}/api/v1/applications/{context.applicationId}/storage/rdd"
    with urlopen(url, timeout=15) as response:
        storage = json.load(response)
    cache_rdd = context._jsc.sc().getPersistentRDDs()
    partitions = {str(rdd["id"]): rdd["numPartitions"] for rdd in storage}
    write_json(run_dir, "storage.json", {
        **metrics,
        "expected_partitions": cached.rdd.getNumPartitions(),
        "persistent_rdd_count": cache_rdd.size(),
        "reported_partitions_by_rdd": partitions,
        "rdds": storage,
    })


def main():
    parser = argparse.ArgumentParser(description="Q1 review length statistics")
    parser.add_argument("--api", choices=("dataframe", "sql"), default="dataframe")
    parser.add_argument("--cache", choices=("none", "filtered", "full"), default="none")
    parser.add_argument("--audit", action="store_true", help="Count eligible reviews and null text by year")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName(f"Q1: {args.api}, cache={args.cache}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    cached = None
    try:
        reviews = spark.read.parquet(str(PARQUET_DIR / "reviews"))
        context = spark.sparkContext
        metrics = {"api": args.api, "cache": args.cache, "application_id": context.applicationId}
        run_dir = os.environ.get("ADVDB_RUN_DIR")
        if args.cache == "full":
            cached = reviews.cache()
            context.setJobGroup("q1-full-materialize", "Q1 full review cache materialization")
            query = cached.agg(
                F.count("*").alias("review_count"),
                F.sum(F.length("text")).alias("text_characters"),
            )
            try:
                rows = query.collect()
            finally:
                record_storage(context, cached, run_dir, metrics)
            write_artifacts("q1-full-materialize", run_dir, query, rows, {
                **metrics, "row_count": len(rows), "cached_columns": reviews.columns,
            })
            return

        filtered = filtered_reviews(spark, reviews, args.api)
        if args.cache == "filtered":
            cached = filtered.cache()
        queries = dataframe_queries(filtered) if args.api == "dataframe" else sql_queries(spark, filtered)
        results = []
        for name, query in zip(("q1-a", "q1-b"), queries):
            context.setJobGroup(name, f"Q1 {args.api}: {name}")
            started = perf_counter()
            rows = query.collect()
            elapsed = perf_counter() - started
            results.append((name, query, rows, elapsed))

        if cached is not None:
            record_storage(context, cached, run_dir, metrics)
        for name, query, rows, elapsed in results:
            write_artifacts(name, run_dir, query, rows, {
                **metrics, "row_count": len(rows), "action_seconds": elapsed,
            })
        write_json(run_dir, "q1-sequence.metrics.json", {
            **metrics, "action_seconds": sum(result[3] for result in results),
        })

        if args.audit:
            audit = eligible_reviews(reviews).groupBy("year").agg(
                F.count("*").alias("eligible_count"),
                F.count_if(F.col("text").isNull()).alias("null_text_count"),
            ).orderBy("year")
            context.setJobGroup("q1-eligibility", "Q1 yearly eligibility audit")
            rows = audit.collect()
            write_artifacts("q1-eligibility", run_dir, audit, rows, {
                **metrics, "row_count": len(rows),
            })
    finally:
        try:
            if cached is not None:
                cached.unpersist()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()

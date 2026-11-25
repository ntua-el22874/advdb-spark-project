import argparse

from pyspark.sql import SparkSession, functions as F

from config import PARQUET_DIR, RAW_DIR


def read_acs(spark):
    return (
        spark.read.schema("zcta STRING, median_household_income INT, population INT")
        .option("header", True)
        .option("sep", ";")
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .csv(str(RAW_DIR / "acs_zcta.csv"))
    )


def read_glove(spark):
    lines = spark.read.text(str(RAW_DIR / "glove.6B.100d.txt"))
    tokens = lines.select(F.split(F.trim("value"), r"\s+").alias("tokens"))
    vector = F.transform(F.slice("tokens", 2, 100), lambda token: token.cast("float"))
    return tokens.select(
        F.col("tokens")[0].alias("word"),
        F.when(F.size("tokens") == 101, vector)
        .otherwise(F.raise_error(F.lit("Expected a GloVe word and 100 coordinates")))
        .alias("vec"),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=("acs", "glove"))
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"Convert {args.source} to Parquet").getOrCreate()
    try:
        data = read_acs(spark) if args.source == "acs" else read_glove(spark)
        data.write.mode("overwrite").parquet(str(PARQUET_DIR / args.source))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

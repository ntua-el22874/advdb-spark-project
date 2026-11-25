from config import PARQUET_DIR, RAW_DIR
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType
)
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Benchmark Q0: JSON vs Parquet") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

json_path = str(RAW_DIR / "yelp_academic_dataset_review.json")
parquet_path = str(PARQUET_DIR / "reviews")

review_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("business_id", StringType(), False),
    StructField("stars", FloatType(), False),
    StructField("useful", IntegerType(), False),
    StructField("funny", IntegerType(), False),
    StructField("cool", IntegerType(), False),
    StructField("text", StringType(), True),
    StructField("date", TimestampType(), False)
])

print("1. ΕΚΤΕΛΕΣΗ ΠΑΝΩ ΣΤΟ ΑΡΧΙΚΟ JSON")

df_json = spark.read.schema(review_schema).json(json_path)
df_json_with_year = df_json.withColumn("year", F.year(F.col("date")))

query_json = df_json_with_year.groupBy("year") \
    .agg(F.avg("stars").alias("avg_stars")) \
    .orderBy("year")

print("\n--- Physical Plan για το JSON ---")
query_json.explain()

t0 = time.time()
query_json.collect()
t_json = time.time() - t0
print(f"\n-> Χρόνος εκτέλεσης με JSON: {t_json:.2f} δευτερόλεπτα\n")

print("2. ΕΚΤΕΛΕΣΗ ΠΑΝΩ ΣΤΟ PARQUET")

df_parquet = spark.read.parquet(parquet_path)

query_parquet = df_parquet.groupBy("year") \
    .agg(F.avg("stars").alias("avg_stars")) \
    .orderBy("year")

print("\n--- Physical Plan για το Parquet ---")
query_parquet.explain()

t0 = time.time()
query_parquet.collect()
t_parquet = time.time() - t0
print(f"\n-> Χρόνος εκτέλεσης με Parquet: {t_parquet:.2f} δευτερόλεπτα\n")

print(f"ΣΥΝΟΨΗ: Το Parquet ήταν κατά {t_json / t_parquet:.1f}x πιο γρήγορο!")

spark.stop()

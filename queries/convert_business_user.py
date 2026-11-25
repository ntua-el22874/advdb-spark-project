from config import PARQUET_DIR, RAW_DIR
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, DoubleType, TimestampType
)

spark = SparkSession.builder \
    .appName("Convert Business and User to Parquet") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

print("\n--- 1. Μετατροπή Business Dataset σε Parquet ---")

business_schema = StructType([
    StructField("business_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("stars", FloatType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("is_open", IntegerType(), True),
    StructField("categories", StringType(), True)
])

input_business_json = str(RAW_DIR / "yelp_academic_dataset_business.json")
output_business_parquet = str(PARQUET_DIR / "business")

df_business = spark.read.schema(business_schema).json(input_business_json)

df_business.write \
    .mode("overwrite") \
    .parquet(output_business_parquet)

print("-> Το Business Parquet δημιουργήθηκε επιτυχώς!")

print("\n--- 2. Μετατροπή User Dataset σε Parquet ---")

user_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("yelping_since", TimestampType(), True),
    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),
    StructField("fans", IntegerType(), True),
    StructField("average_stars", FloatType(), True)
])

input_user_json = str(RAW_DIR / "yelp_academic_dataset_user.json")
output_user_parquet = str(PARQUET_DIR / "users")

df_user = spark.read.schema(user_schema).json(input_user_json)

df_user.write \
    .mode("overwrite") \
    .parquet(output_user_parquet)

print("-> Το User Parquet δημιουργήθηκε επιτυχώς!")

spark.stop()
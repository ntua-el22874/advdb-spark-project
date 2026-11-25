from config import PARQUET_DIR, RAW_DIR
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Convert Reviews to Parquet") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

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

input_json = str(RAW_DIR / "yelp_academic_dataset_review.json")
output_parquet = str(PARQUET_DIR / "reviews")

df_raw = spark.read.schema(review_schema).json(input_json)

df_with_year = df_raw.withColumn("year", F.year(F.col("date")))

df_with_year.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(output_parquet)

spark.stop()

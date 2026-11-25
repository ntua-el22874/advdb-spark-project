from config import PARQUET_DIR
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Query 1 - Review Length and Ratings") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

parquet_path = str(PARQUET_DIR / "reviews")

df_reviews = spark.read.parquet(parquet_path)

df_filtered = df_reviews.filter(
    (F.col("year") >= 2015) &
    (F.col("year") <= 2021) &
    (F.col("useful") >= 1)
)

char_len = F.length(F.col("text"))

df_categorized = df_filtered.withColumn(
    "category",
    F.when(char_len < 250, "Short")
     .when((char_len >= 250) & (char_len <= 499), "Medium")
     .when((char_len >= 500) & (char_len <= 999), "Long")
     .otherwise("Very Long")
)

df_result = df_categorized.groupBy("category") \
    .agg(
        F.count("*").alias("review_count"),
        F.round(F.avg("stars"), 2).alias("avg_stars")
    ) \
    .orderBy("review_count", ascending=False)

print("\n--- Physical Plan (Χωρίς Cache) ---")
df_result.explain()

t0 = time.time()
df_result.show()
t_no_cache = time.time() - t0
print(f"-> Χρόνος εκτέλεσης ΧΩΡΙΣ cache: {t_no_cache:.2f} δευτερόλεπτα\n")

df_categorized.cache()

t0 = time.time()
df_result.show()
t_cache_run1 = time.time() - t0
print(f"-> Χρόνος εκτέλεσης με Cache (1η φορά - Φόρτωση στη RAM): {t_cache_run1:.2f} δευτερόλεπτα")

t0 = time.time()
df_result.show()
t_cache_run2 = time.time() - t0
print(f"-> Χρόνος εκτέλεσης με Cache (2η φορά - Απευθείας από RAM): {t_cache_run2:.2f} δευτερόλεπτα\n")

df_categorized.unpersist()

spark.stop()

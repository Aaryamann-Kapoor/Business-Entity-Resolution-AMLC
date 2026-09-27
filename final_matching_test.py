from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("FinalTestMatching")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "100")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read scored test candidates
df = spark.read.parquet("data/test_scored_source2")

print("Total candidate rows:", df.count())

# Final score
df = df.withColumn(
    "final_score",
    (
        F.col("name_similarity") * 0.30
        + F.col("exact_name_match") * 0.25
        + F.col("exact_core_match") * 0.25
        + F.col("exact_city_match") * 0.10
        + F.col("exact_country_match") * 0.10
    )
)

# Rank candidates for every S2 record
window = Window.partitionBy(
    "source_entity_id"
).orderBy(
    F.col("final_score").desc(),
    F.col("name_similarity").desc()
)

ranked = df.withColumn(
    "rank",
    F.row_number().over(window)
)

# Best candidate
best = ranked.filter(
    F.col("rank") == 1
)

# Temporary decision threshold
predictions = best.withColumn(
    "decision",
    F.when(
        F.col("final_score") >= 0.70,
        "MATCH"
    ).otherwise(
        "UNMATCHED"
    )
)

# Show predictions
print("\n==============================================")
print("SOURCE 2 TEST PREDICTIONS")
print("==============================================")

predictions.select(
    "source_entity_id",
    "source_business_name",
    "candidate_entity_id",
    "candidate_business_name",
    "name_similarity",
    "exact_name_match",
    "exact_core_match",
    "exact_city_match",
    "exact_country_match",
    "final_score",
    "decision"
).orderBy(
    F.col("final_score").desc()
).show(
    50,
    truncate=False
)

# Summary
total = predictions.count()

matched = predictions.filter(
    F.col("decision") == "MATCH"
).count()

unmatched = predictions.filter(
    F.col("decision") == "UNMATCHED"
).count()

print("\n==============================================")
print("TEST SUMMARY")
print("==============================================")

print("Test source records :", total)
print("Predicted MATCH     :", matched)
print("Predicted UNMATCHED:", unmatched)

if total > 0:
    print(
        "Match coverage      :",
        round((matched / total) * 100, 2),
        "%"
    )

# Save predictions
predictions.select(
    "source_entity_id",
    "source_business_name",
    "candidate_entity_id",
    "candidate_business_name",
    "final_score",
    "decision"
).write \
    .mode("overwrite") \
    .option("header", True) \
    .option("sep", "\t") \
    .csv("data/final_test_source2")

print("\nSaved:")
print("data/final_test_source2")

spark.stop()
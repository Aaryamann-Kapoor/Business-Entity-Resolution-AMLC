from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("EvaluateTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# READ TEST SCORES
# ============================================================

pred = (
    spark.read.parquet("data/test_scored_source2")
    .filter(F.col("source_entity_id") != "entity_id")
)

print("Valid candidate pairs:", pred.count())

# ============================================================
# READ GOLDEN TRUTH
# ============================================================

truth = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .csv("train_ground_truth.tsv")
)

# ============================================================
# REVERSE GOLDEN TRUTH
#
# S1 -> S2,S3
#
# becomes
#
# S2/S3 -> S1
# ============================================================

truth_rows = []

for row in truth.collect():

    s1_id = row["source1_entity_id"]
    matched_ids = row["matched_entity_ids"]

    if matched_ids:

        for matched_id in matched_ids.split(","):

            matched_id = matched_id.strip()

            truth_rows.append(
                (matched_id, s1_id)
            )

truth_reverse = spark.createDataFrame(
    truth_rows,
    ["source_entity_id", "true_s1_entity_id"]
)

print(
    "Golden-truth S2/S3 relationships:",
    truth_reverse.count()
)

# ============================================================
# JOIN PREDICTIONS WITH GOLDEN TRUTH
# ============================================================

evaluated = pred.join(
    truth_reverse,
    on="source_entity_id",
    how="left"
)

# ============================================================
# SHOW EXAMPLES
# ============================================================

print("\n==============================================")
print("TEST PREDICTIONS VS GOLDEN TRUTH")
print("==============================================")

evaluated.select(
    "source_entity_id",
    "candidate_entity_id",
    "true_s1_entity_id",
    "name_similarity",
    "exact_name_match",
    "exact_core_match",
    "exact_city_match",
    "exact_country_match"
).show(
    30,
    truncate=False
)

spark.stop()
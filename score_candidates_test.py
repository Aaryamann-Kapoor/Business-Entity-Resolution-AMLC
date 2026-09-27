from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BusinessEntityCandidateScoringTest")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "100")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# READ TEST SOURCE 2 CANDIDATES
# ============================================================

source2 = spark.read.option("header", True).option("sep", "\t").csv(
    "data/test_candidates/source2_candidates.tsv"
)

print("\nSource 2 candidate rows:", source2.count())


# ============================================================
# NAME NORMALIZATION
# ============================================================

def add_name_tokens(df):

    df = df.withColumn(
        "source_name_clean",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("source_business_name"),
                        F.lit("")
                    ),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )

    df = df.withColumn(
        "candidate_name_clean",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("candidate_business_name"),
                        F.lit("")
                    ),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )

    return df


# ============================================================
# SIMILARITY
# ============================================================

def add_similarity(df):

    df = add_name_tokens(df)

    # Name lengths
    df = df.withColumn(
        "source_name_length",
        F.length(F.col("source_name_clean"))
    )

    df = df.withColumn(
        "candidate_name_length",
        F.length(F.col("candidate_name_clean"))
    )

    # Levenshtein distance
    df = df.withColumn(
        "name_levenshtein",
        F.levenshtein(
            F.col("source_name_clean"),
            F.col("candidate_name_clean")
        )
    )

    # Maximum length
    df = df.withColumn(
        "max_name_length",
        F.greatest(
            F.col("source_name_length"),
            F.col("candidate_name_length")
        )
    )

    # Name similarity
    df = df.withColumn(
        "name_similarity",
        F.when(
            F.col("max_name_length") == 0,
            F.lit(0.0)
        ).otherwise(
            1.0 -
            (
                F.col("name_levenshtein")
                /
                F.col("max_name_length")
            )
        )
    )

    # Exact normalized name
    df = df.withColumn(
        "exact_name_match",
        F.when(
            F.col("source_name_clean")
            ==
            F.col("candidate_name_clean"),
            1
        ).otherwise(0)
    )

    # Exact core name
    df = df.withColumn(
        "exact_core_match",
        F.when(
            F.col("source_core_name")
            ==
            F.col("candidate_core_name"),
            1
        ).otherwise(0)
    )

    # Exact city
    df = df.withColumn(
        "exact_city_match",
        F.when(
            F.col("source_city")
            ==
            F.col("candidate_city"),
            1
        ).otherwise(0)
    )

    # Exact country
    df = df.withColumn(
        "exact_country_match",
        F.when(
            F.col("source_country")
            ==
            F.col("candidate_country"),
            1
        ).otherwise(0)
    )

    return df


# ============================================================
# SCORE SOURCE 2
# ============================================================

print("\nScoring SOURCE 2...")

source2_scored = add_similarity(source2)


# ============================================================
# SHOW RESULTS
# ============================================================

print("\n==============================================")
print("SOURCE 2 TEST SCORES")
print("==============================================")

source2_scored.select(
    "source_entity_id",
    "source_business_name",
    "candidate_entity_id",
    "candidate_business_name",
    "source_core_name",
    "candidate_core_name",
    "source_city",
    "candidate_city",
    "name_levenshtein",
    "name_similarity",
    "exact_name_match",
    "exact_core_match",
    "exact_city_match",
    "exact_country_match"
).show(
    30,
    truncate=False
)


# ============================================================
# SAVE TEST SCORES
# ============================================================

print("\nSaving test scores...")

source2_scored.write \
    .mode("overwrite") \
    .parquet(
        "data/test_scored_source2"
    )


print("\n==============================================")
print("SOURCE 2 TEST SCORING COMPLETE")
print("==============================================")

print("Saved:")
print("data/test_scored_source2")

spark.stop()
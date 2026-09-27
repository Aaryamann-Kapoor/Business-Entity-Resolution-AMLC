from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BusinessEntityCandidateScoring")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# READ CANDIDATES
# ============================================================

source2 = spark.read.option("header", True).option("sep", "\t").csv(
    "data/test_candidates/source2_candidates.tsv"
)

source3 = spark.read.option("header", True).option("sep", "\t").csv(
    "data/test_candidates/source3_candidates.tsv"
)


# ============================================================
# NAME NORMALIZATION FOR SIMILARITY
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
        "master_name_clean",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("master_business_name"),
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
# CHARACTER-BASED SIMILARITY
#
# Spark built-in levenshtein
# ============================================================

def add_similarity(df):

    df = add_name_tokens(df)

    # --------------------------------------------------------
    # Name lengths
    # --------------------------------------------------------

    df = df.withColumn(
        "source_name_length",
        F.length(F.col("source_name_clean"))
    )

    df = df.withColumn(
        "master_name_length",
        F.length(F.col("master_name_clean"))
    )

    # --------------------------------------------------------
    # Levenshtein distance
    # --------------------------------------------------------

    df = df.withColumn(
        "name_levenshtein",
        F.levenshtein(
            F.col("source_name_clean"),
            F.col("master_name_clean")
        )
    )

    # --------------------------------------------------------
    # Maximum name length
    # --------------------------------------------------------

    df = df.withColumn(
        "max_name_length",
        F.greatest(
            F.col("source_name_length"),
            F.col("master_name_length")
        )
    )

    # --------------------------------------------------------
    # Similarity
    #
    # 1.0 = identical
    # 0.0 = very different
    # --------------------------------------------------------

    df = df.withColumn(
        "name_similarity",

        F.when(
            F.col("max_name_length") == 0,
            F.lit(0.0)
        ).otherwise(

            1.0
            -
            (
                F.col("name_levenshtein")
                /
                F.col("max_name_length")
            )
        )
    )

    # --------------------------------------------------------
    # Exact normalized name
    # --------------------------------------------------------

    df = df.withColumn(
        "exact_name_match",
        F.when(
            F.col("source_name_clean")
            ==
            F.col("master_name_clean"),
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # Exact core name
    # --------------------------------------------------------

    df = df.withColumn(
        "exact_core_match",
        F.when(
            F.col("source_core_name")
            ==
            F.col("master_core_name"),
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # Exact city
    # --------------------------------------------------------

    df = df.withColumn(
        "exact_city_match",
        F.when(
            F.col("source_city")
            ==
            F.col("master_city"),
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # Exact country
    # --------------------------------------------------------

    df = df.withColumn(
        "exact_country_match",
        F.when(
            F.col("source_country")
            ==
            F.col("master_country"),
            1
        ).otherwise(0)
    )

    return df


# ============================================================
# SCORE SOURCE 2
# ============================================================

print("\nScoring SOURCE 2...")

source2_scored = add_similarity(
    source2
)


# ============================================================
# SCORE SOURCE 3
# ============================================================

print("\nScoring SOURCE 3...")

source3_scored = add_similarity(
    source3
)


# ============================================================
# DISPLAY SOURCE 2
# ============================================================

print("\n==============================================")
print("SOURCE 2 SCORES")
print("==============================================")

source2_scored.select(
    "source_entity_id",
    "source_business_name",
    "master_entity_id",
    "master_business_name",
    "source_core_name",
    "master_core_name",
    "source_city",
    "master_city",
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
# DISPLAY SOURCE 3
# ============================================================

print("\n==============================================")
print("SOURCE 3 SCORES")
print("==============================================")

source3_scored.select(
    "source_entity_id",
    "source_business_name",
    "master_entity_id",
    "master_business_name",
    "source_core_name",
    "master_core_name",
    "source_city",
    "master_city",
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
# SAVE SCORES
# ============================================================

print("\nSaving scored candidates...")


(
    source2_scored
    .write
    .mode("overwrite")
    .parquet(
        "data/scored_source2"
    )
)


(
    source3_scored
    .write
    .mode("overwrite")
    .parquet(
        "data/scored_source3"
    )
)


print("\n==============================================")
print("SCORING COMPLETE")
print("==============================================")

print("Saved:")
print("data/scored_source2")
print("data/scored_source3")


spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BusinessEntityResolution")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "100")
    .config("spark.sql.autoBroadcastJoinThreshold", "50m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# PATHS
# ============================================================

MASTER_PATH = "data/processed_master"
SOURCE2_PATH = "data/processed_source2"
SOURCE3_PATH = "data/processed_source3"

OUTPUT_PATH = "data/final_matches"


# ============================================================
# READ MASTER
# ============================================================

print("\nReading MASTER...")

master = spark.read.parquet(MASTER_PATH)


# ============================================================
# READ SOURCE 2
# ============================================================

print("Reading SOURCE 2...")

source2 = spark.read.parquet(SOURCE2_PATH)


# ============================================================
# READ SOURCE 3
# ============================================================

print("Reading SOURCE 3...")

source3 = spark.read.parquet(SOURCE3_PATH)


# ============================================================
# MASTER BLOCK TABLE
# ============================================================

master_block = master.select(
    "entity_id",
    "business_name",
    "business_address",
    "core_name",
    "city",
    "country"
).withColumn(
    "block_key",
    F.concat_ws(
        "|",
        F.col("core_name"),
        F.col("city"),
        F.upper(F.trim(F.col("country")))
    )
)


# ============================================================
# FUNCTION TO MATCH ONE SOURCE
# ============================================================

def match_source(source_df, source_name):

    print("\n========================================")
    print("PROCESSING", source_name)
    print("========================================")

    # --------------------------------------------------------
    # Prepare source
    # --------------------------------------------------------

    source = source_df.select(
        "entity_id",
        "business_name",
        "business_address",
        "core_name",
        "city",
        "country"
    )

    source = source.withColumn(
        "block_key",
        F.concat_ws(
            "|",
            F.col("core_name"),
            F.col("city"),
            F.upper(F.trim(F.col("country")))
        )
    )

    # --------------------------------------------------------
    # Remove records that cannot participate in blocking
    # --------------------------------------------------------

    source = source.filter(
        (F.col("core_name") != "") &
        (F.col("city") != "") &
        (F.col("country") != "")
    )

    # --------------------------------------------------------
    # JOIN ONLY ON BLOCK KEY
    # --------------------------------------------------------

    candidates = (
        source.alias("s")
        .join(
            master_block.alias("m"),
            F.col("s.block_key") == F.col("m.block_key"),
            "inner"
        )
    )

    # --------------------------------------------------------
    # NAME CLEANING
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "source_name_clean",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("s.business_name"),
                        F.lit("")
                    ),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )

    candidates = candidates.withColumn(
        "master_name_clean",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("m.business_name"),
                        F.lit("")
                    ),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )

    # --------------------------------------------------------
    # LEVENSHTEIN
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "name_distance",
        F.levenshtein(
            F.col("source_name_clean"),
            F.col("master_name_clean")
        )
    )

    # --------------------------------------------------------
    # NAME LENGTH
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "max_name_length",
        F.greatest(
            F.length(F.col("source_name_clean")),
            F.length(F.col("master_name_clean"))
        )
    )

    # --------------------------------------------------------
    # NAME SIMILARITY
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "name_similarity",
        F.when(
            F.col("max_name_length") == 0,
            F.lit(0.0)
        ).otherwise(
            1.0 -
            (
                F.col("name_distance")
                /
                F.col("max_name_length")
            )
        )
    )

    # --------------------------------------------------------
    # CORE NAME MATCH
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "core_match",
        F.when(
            F.col("s.core_name") ==
            F.col("m.core_name"),
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # CITY MATCH
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "city_match",
        F.when(
            F.col("s.city") ==
            F.col("m.city"),
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # COUNTRY MATCH
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "country_match",
        F.when(
            F.upper(F.col("s.country")) ==
            F.upper(F.col("m.country")),
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # FINAL SCORE
    #
    # Because the blocking key already guarantees:
    #
    # core_name + city + country
    #
    # we give name similarity the main role.
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "match_score",

        (
            F.col("name_similarity") * F.lit(0.70)
            +
            F.col("core_match") * F.lit(0.15)
            +
            F.col("city_match") * F.lit(0.10)
            +
            F.col("country_match") * F.lit(0.05)
        )
    )

    # --------------------------------------------------------
    # BEST MASTER MATCH PER SOURCE RECORD
    # --------------------------------------------------------

    window = Window.partitionBy(
        F.col("s.entity_id")
    ).orderBy(
        F.col("match_score").desc(),
        F.col("name_similarity").desc()
    )

    ranked = candidates.withColumn(
        "rank",
        F.row_number().over(window)
    )

    best = ranked.filter(
        F.col("rank") == 1
    )

    # --------------------------------------------------------
    # FINAL COLUMNS
    # --------------------------------------------------------

    result = best.select(
        F.lit(source_name).alias("source"),
        F.col("s.entity_id").alias("source_entity_id"),
        F.col("s.business_name").alias("source_business_name"),
        F.col("s.business_address").alias("source_address"),

        F.col("m.entity_id").alias("master_entity_id"),
        F.col("m.business_name").alias("master_business_name"),
        F.col("m.business_address").alias("master_address"),

        F.col("s.core_name").alias("core_name"),
        F.col("s.city").alias("city"),
        F.col("s.country").alias("country"),

        F.col("name_similarity"),
        F.col("match_score")
    )

    # --------------------------------------------------------
    # SHOW SAMPLE
    # --------------------------------------------------------

    print("\n", source_name, "BEST MATCHES")

    result.show(
        20,
        truncate=False
    )

    return result


# ============================================================
# SOURCE 2
# ============================================================

result2 = match_source(
    source2,
    "SOURCE2"
)


# ============================================================
# SOURCE 3
# ============================================================

result3 = match_source(
    source3,
    "SOURCE3"
)


# ============================================================
# COMBINE SOURCE 2 + SOURCE 3
# ============================================================

print("\n========================================")
print("COMBINING RESULTS")
print("========================================")

final_result = result2.unionByName(
    result3
)


# ============================================================
# SAVE FINAL RESULT AS TSV
# ============================================================

OUTPUT_PATH = "data/final_matches.tsv"

print("\nWriting final TSV...")

(
    final_result
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("sep", "\t")
    .option("quote", '"')
    .option("escape", '"')
    .csv(OUTPUT_PATH)
)


# ============================================================
# FINISH
# ============================================================

print("\n========================================")
print("MATCHING COMPLETE")
print("========================================")

print("Output directory:")
print(OUTPUT_PATH)

spark.stop()
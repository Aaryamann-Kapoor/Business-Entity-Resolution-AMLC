import argparse
import os
import re
import unicodedata

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# ARGUMENTS
# ============================================================

parser = argparse.ArgumentParser(
    description="Business Entity Resolution"
)

parser.add_argument("--source1", required=True)
parser.add_argument("--source2", required=True)
parser.add_argument("--source3", required=True)
parser.add_argument(
    "--output",
    default="results"
)

args = parser.parse_args()


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BusinessEntityResolution")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.autoBroadcastJoinThreshold", "50m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# READ TSV
# ============================================================

def read_source(path):

    print("\nReading:")
    print(path)

    return (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .option("inferSchema", False)
        .csv(path)
    )


master = read_source(args.source1)
source2 = read_source(args.source2)
source3 = read_source(args.source3)


# ============================================================
# NORMALIZATION
# ============================================================

LEGAL_SUFFIXES = [
    "PRIVATE LIMITED",
    "PVT LTD",
    "PVT LIMITED",
    "LIMITED",
    "LTD",
    "INCORPORATED",
    "INC",
    "CORPORATION",
    "CORP",
    "COMPANY",
    "CO",
    "LLC",
]


def normalize_name_column(df):

    df = df.withColumn(
        "business_name",
        F.trim(
            F.regexp_replace(
                F.coalesce(
                    F.col("business_name"),
                    F.lit("")
                ),
                r"\s+",
                " "
            )
        )
    )

    # Normalize punctuation
    df = df.withColumn(
        "name_normalized",
        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("business_name"),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )

    # Remove legal suffixes from the end
    suffix_pattern = (
        r"\s+(?:"
        + "|".join(
            re.escape(x)
            for x in sorted(
                LEGAL_SUFFIXES,
                key=len,
                reverse=True
            )
        )
        + r")$"
    )

    df = df.withColumn(
        "core_name",
        F.trim(
            F.regexp_replace(
                F.col("name_normalized"),
                suffix_pattern,
                ""
            )
        )
    )

    return df


# ============================================================
# CITY EXTRACTION
#
# Conservative extraction:
# use the final comma-separated address component
# ============================================================

def add_city(df):

    df = df.withColumn(
        "business_address",
        F.coalesce(
            F.col("business_address"),
            F.lit("")
        )
    )

    df = df.withColumn(
        "city",
        F.upper(
            F.trim(
                F.element_at(
                    F.split(
                        F.col("business_address"),
                        ","
                    ),
                    -1
                )
            )
        )
    )

    return df


# ============================================================
# PREPARE DATA
# ============================================================

def prepare(df):

    df = normalize_name_column(df)

    df = add_city(df)

    df = df.withColumn(
        "country",
        F.upper(
            F.trim(
                F.coalesce(
                    F.col("country"),
                    F.lit("")
                )
            )
        )
    )

    return df


master = prepare(master)
source2 = prepare(source2)
source3 = prepare(source3)


# ============================================================
# MASTER BLOCK
# ============================================================

master_block = (
    master
    .select(
        "entity_id",
        "business_name",
        "business_address",
        "core_name",
        "city",
        "country"
    )
    .withColumn(
        "block_key",
        F.concat_ws(
            "|",
            F.col("core_name"),
            F.col("city"),
            F.col("country")
        )
    )
)


# ============================================================
# MATCH FUNCTION
# ============================================================

def match_source(source_df, source_label):

    print("\n==============================================")
    print("PROCESSING", source_label)
    print("==============================================")

    source = (
        source_df
        .select(
            "entity_id",
            "business_name",
            "business_address",
            "core_name",
            "city",
            "country"
        )
        .withColumn(
            "block_key",
            F.concat_ws(
                "|",
                F.col("core_name"),
                F.col("city"),
                F.col("country")
            )
        )
    )

    # Remove unusable blocking records
    source = source.filter(
        (F.col("core_name") != "") &
        (F.col("city") != "") &
        (F.col("country") != "")
    )

    # --------------------------------------------------------
    # BLOCK
    # --------------------------------------------------------

    candidates = (
        source.alias("s")
        .join(
            master_block.alias("m"),
            F.col("s.block_key") ==
            F.col("m.block_key"),
            "inner"
        )
    )

    # --------------------------------------------------------
    # CLEAN NAMES
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

    candidates = candidates.withColumn(
        "max_name_length",
        F.greatest(
            F.length(F.col("source_name_clean")),
            F.length(F.col("master_name_clean"))
        )
    )

    candidates = candidates.withColumn(
        "name_similarity",
        F.when(
            F.col("max_name_length") == 0,
            F.lit(0.0)
        ).otherwise(
            1.0 -
            (
                F.col("name_distance") /
                F.col("max_name_length")
            )
        )
    )

    # --------------------------------------------------------
    # FEATURES
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "core_match",
        F.when(
            F.col("s.core_name") ==
            F.col("m.core_name"),
            1
        ).otherwise(0)
    )

    candidates = candidates.withColumn(
        "city_match",
        F.when(
            F.col("s.city") ==
            F.col("m.city"),
            1
        ).otherwise(0)
    )

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
    # BEST MATCH
    # --------------------------------------------------------

    window = (
        Window
        .partitionBy(F.col("s.entity_id"))
        .orderBy(
            F.col("match_score").desc(),
            F.col("name_similarity").desc()
        )
    )

    ranked = candidates.withColumn(
        "rank",
        F.row_number().over(window)
    )

    best = ranked.filter(
        F.col("rank") == 1
    )

    return best.select(
        F.lit(source_label).alias("source"),
        F.col("s.entity_id").alias(
            "source_entity_id"
        ),
        F.col("s.business_name").alias(
            "source_business_name"
        ),
        F.col("s.business_address").alias(
            "source_business_address"
        ),
        F.col("s.country").alias(
            "source_country"
        ),

        F.col("m.entity_id").alias(
            "matched_entity_id"
        ),
        F.col("m.business_name").alias(
            "matched_business_name"
        ),
        F.col("m.business_address").alias(
            "matched_business_address"
        ),

        F.col("name_similarity"),
        F.col("match_score")
    )


# ============================================================
# RUN
# ============================================================

result2 = match_source(
    source2,
    "SOURCE2"
)

result3 = match_source(
    source3,
    "SOURCE3"
)


# ============================================================
# COMBINE
# ============================================================

final_result = result2.unionByName(result3)


# ============================================================
# OUTPUT
# ============================================================

os.makedirs(args.output, exist_ok=True)

output_file = os.path.join(
    args.output,
    "matching_results.tsv"
)

print("\nWriting results...")

(
    final_result
    .write
    .mode("overwrite")
    .option("header", True)
    .option("sep", "\t")
    .option("quote", '"')
    .option("escape", '"')
    .csv(output_file)
)


print("\n==============================================")
print("MATCHING COMPLETE")
print("==============================================")

print("Output:", output_file)

spark.stop()
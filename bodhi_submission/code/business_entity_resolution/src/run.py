import argparse
import glob
import os
import re
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# ARGUMENTS
# ============================================================

parser = argparse.ArgumentParser(
    description="Business Entity Resolution — Team Bodhi"
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
    # English
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
    "LLP",
    "LP",
    "PLC",
    # German
    "GMBH",
    "AG",
    # French / Spanish / Italian / Portuguese
    "SA",
    "SAS",
    "SARL",
    "SRL",
    "SL",
    # Dutch / Belgian
    "NV",
    "BV",
    # Australian
    "PTY",
    "PTY LTD",
    # Malaysian
    "SDN BHD",
    "BHD",
    # Japanese
    "KK",
    # Finnish
    "OY",
    "OYJ",
    # Swedish / Nordic
    "AB",
    # Turkish
    "AS",
]


def normalize_name_column(df):
    """
    Normalize business names:
      1. Collapse whitespace
      2. Strip punctuation
      3. Uppercase
      4. Remove legal suffixes → core_name
      5. Extract name_prefix (first 5 chars)
      6. Tokenize for Jaccard
    """

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

    # Strip punctuation, uppercase
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

    # Name prefix: first 5 characters of core_name
    # (used for loose blocking)
    df = df.withColumn(
        "name_prefix",
        F.substring(F.col("core_name"), 1, 5)
    )

    # Word tokens for Jaccard similarity
    df = df.withColumn(
        "name_tokens",
        F.split(
            F.lower(F.col("core_name")),
            r"\s+"
        )
    )

    return df


# ============================================================
# CITY EXTRACTION
#
# Improved: if the last comma-component looks like a
# zip / postal code (all digits), fall back to
# the second-to-last component.
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
        "_addr_parts",
        F.split(F.col("business_address"), ",")
    )

    df = df.withColumn(
        "_raw_last",
        F.upper(
            F.trim(
                F.element_at(
                    F.col("_addr_parts"),
                    -1
                )
            )
        )
    )

    # If last component is a zip code (all digits)
    # and there are at least 2 components, take
    # the second-to-last instead
    df = df.withColumn(
        "city",
        F.when(
            (F.size(F.col("_addr_parts")) > 1) &
            F.col("_raw_last").rlike(r"^\d+$"),
            F.upper(
                F.trim(
                    F.element_at(
                        F.col("_addr_parts"),
                        -2
                    )
                )
            )
        ).otherwise(F.col("_raw_last"))
    )

    df = df.drop("_addr_parts", "_raw_last")

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

    # Clean address for similarity comparison
    df = df.withColumn(
        "address_clean",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("business_address"),
                        F.lit("")
                    ),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )

    return df


master = prepare(master)
source2 = prepare(source2)
source3 = prepare(source3)


# ============================================================
# MATCH FUNCTION
# ============================================================

# Minimum score to accept a match
MATCH_THRESHOLD = 0.45


def match_source(source_df, source_label):
    """
    Multi-pass blocking → similarity scoring → best-match
    selection for one source dataset against master.

    Returns (best_matches_df, candidate_pairs_df).
    """

    print("\n==============================================")
    print("PROCESSING", source_label)
    print("==============================================")

    select_cols = [
        "entity_id",
        "business_name",
        "business_address",
        "core_name",
        "name_prefix",
        "name_tokens",
        "city",
        "country",
        "address_clean"
    ]

    m = master.select(*select_cols)
    s = source_df.select(*select_cols)

    # --------------------------------------------------------
    # MULTI-PASS BLOCKING
    #
    # Pass 1: core_name + country
    #         (catches city typos / missing city)
    #
    # Pass 2: name_prefix(5) + city + country
    #         (catches core-name typos that share
    #          the same prefix)
    #
    # Pass 3: core_name + city
    #         (catches country mismatches /
    #          multinational entities)
    # --------------------------------------------------------

    def block_join(source, master, join_cond, label):
        return (
            source.alias("s")
            .join(
                master.alias("m"),
                join_cond,
                "inner"
            )
            .withColumn(
                "block_type",
                F.lit(label)
            )
        )

    block1 = block_join(
        s, m,
        (F.col("s.core_name") == F.col("m.core_name")) &
        (F.col("s.country") == F.col("m.country")) &
        (F.col("s.core_name") != "") &
        (F.col("s.country") != ""),
        "core_name+country"
    )

    block2 = block_join(
        s, m,
        (F.col("s.name_prefix") ==
         F.col("m.name_prefix")) &
        (F.col("s.city") == F.col("m.city")) &
        (F.col("s.country") == F.col("m.country")) &
        (F.col("s.name_prefix") != "") &
        (F.col("s.city") != "") &
        (F.col("s.country") != ""),
        "prefix+city+country"
    )

    block3 = block_join(
        s, m,
        (F.col("s.core_name") == F.col("m.core_name")) &
        (F.col("s.city") == F.col("m.city")) &
        (F.col("s.core_name") != "") &
        (F.col("s.city") != ""),
        "core_name+city"
    )

    # --------------------------------------------------------
    # Flatten all blocks into uniform columns and
    # deduplicate (source_id, master_id) pairs
    # --------------------------------------------------------

    pair_cols = [
        F.col("s.entity_id").alias(
            "source_entity_id"
        ),
        F.col("s.business_name").alias(
            "source_business_name"
        ),
        F.col("s.business_address").alias(
            "source_business_address"
        ),
        F.col("s.core_name").alias(
            "source_core_name"
        ),
        F.col("s.name_tokens").alias(
            "source_name_tokens"
        ),
        F.col("s.city").alias(
            "source_city"
        ),
        F.col("s.country").alias(
            "source_country"
        ),
        F.col("s.address_clean").alias(
            "source_address_clean"
        ),

        F.col("m.entity_id").alias(
            "master_entity_id"
        ),
        F.col("m.business_name").alias(
            "master_business_name"
        ),
        F.col("m.business_address").alias(
            "master_business_address"
        ),
        F.col("m.core_name").alias(
            "master_core_name"
        ),
        F.col("m.name_tokens").alias(
            "master_name_tokens"
        ),
        F.col("m.city").alias(
            "master_city"
        ),
        F.col("m.country").alias(
            "master_country"
        ),
        F.col("m.address_clean").alias(
            "master_address_clean"
        ),

        F.col("block_type")
    ]

    candidates = (
        block1.select(*pair_cols)
        .unionByName(
            block2.select(*pair_cols)
        )
        .unionByName(
            block3.select(*pair_cols)
        )
        .dropDuplicates([
            "source_entity_id",
            "master_entity_id"
        ])
    )

    print("Candidate pairs generated")

    # --------------------------------------------------------
    # SAVE CANDIDATE PAIRS (before scoring)
    # --------------------------------------------------------

    candidate_pairs_out = candidates.select(
        F.lit(source_label).alias("source"),
        "source_entity_id",
        "source_business_name",
        "source_core_name",
        "source_city",
        "source_country",
        "master_entity_id",
        "master_business_name",
        "master_core_name",
        "master_city",
        "master_country",
        "block_type"
    )

    # --------------------------------------------------------
    # CLEAN NAMES FOR SIMILARITY
    # --------------------------------------------------------

    candidates = candidates.withColumn(
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

    candidates = candidates.withColumn(
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

    # --------------------------------------------------------
    # LEVENSHTEIN NAME SIMILARITY
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
            F.length(F.col("master_name_clean")),
            F.lit(1)
        )
    )

    candidates = candidates.withColumn(
        "name_similarity",
        1.0 - (
            F.col("name_distance") /
            F.col("max_name_length")
        )
    )

    # --------------------------------------------------------
    # JACCARD TOKEN SIMILARITY
    #
    # Handles word reordering and extra words
    # e.g. "Steel Authority India" vs
    #      "India Steel Authority" → high Jaccard
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "intersection_size",
        F.size(
            F.array_intersect(
                F.col("source_name_tokens"),
                F.col("master_name_tokens")
            )
        )
    )

    candidates = candidates.withColumn(
        "union_size",
        F.size(
            F.array_union(
                F.col("source_name_tokens"),
                F.col("master_name_tokens")
            )
        )
    )

    candidates = candidates.withColumn(
        "jaccard_similarity",
        F.when(
            F.col("union_size") == 0,
            F.lit(0.0)
        ).otherwise(
            F.col("intersection_size") /
            F.col("union_size")
        )
    )

    # --------------------------------------------------------
    # ADDRESS SIMILARITY
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "address_distance",
        F.levenshtein(
            F.col("source_address_clean"),
            F.col("master_address_clean")
        )
    )

    candidates = candidates.withColumn(
        "max_address_length",
        F.greatest(
            F.length(F.col("source_address_clean")),
            F.length(F.col("master_address_clean")),
            F.lit(1)
        )
    )

    candidates = candidates.withColumn(
        "address_similarity",
        F.when(
            (F.length(F.col("source_address_clean"))
             <= 1) |
            (F.length(F.col("master_address_clean"))
             <= 1),
            F.lit(0.0)
        ).otherwise(
            1.0 - (
                F.col("address_distance") /
                F.col("max_address_length")
            )
        )
    )

    # --------------------------------------------------------
    # BINARY FEATURES
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "core_match",
        F.when(
            F.col("source_core_name") ==
            F.col("master_core_name"),
            1
        ).otherwise(0)
    )

    candidates = candidates.withColumn(
        "city_match",
        F.when(
            F.col("source_city") ==
            F.col("master_city"),
            1
        ).otherwise(0)
    )

    candidates = candidates.withColumn(
        "country_match",
        F.when(
            F.col("source_country") ==
            F.col("master_country"),
            1
        ).otherwise(0)
    )

    # --------------------------------------------------------
    # FINAL SCORE
    #
    # name_similarity    0.35  (char-level)
    # jaccard_similarity 0.25  (token-level)
    # core_match         0.15  (exact core name)
    # address_similarity 0.10  (char-level address)
    # city_match         0.10  (exact city)
    # country_match      0.05  (exact country)
    # --------------------------------------------------------

    candidates = candidates.withColumn(
        "match_score",
        (
            F.col("name_similarity")
            * F.lit(0.35)

            + F.col("jaccard_similarity")
            * F.lit(0.25)

            + F.col("core_match")
            * F.lit(0.15)

            + F.col("address_similarity")
            * F.lit(0.10)

            + F.col("city_match")
            * F.lit(0.10)

            + F.col("country_match")
            * F.lit(0.05)
        )
    )

    # --------------------------------------------------------
    # BEST MATCH PER SOURCE ENTITY
    # --------------------------------------------------------

    window = (
        Window
        .partitionBy("source_entity_id")
        .orderBy(
            F.col("match_score").desc(),
            F.col("name_similarity").desc(),
            F.col("jaccard_similarity").desc()
        )
    )

    ranked = candidates.withColumn(
        "rank",
        F.row_number().over(window)
    )

    best = ranked.filter(
        F.col("rank") == 1
    )

    # --------------------------------------------------------
    # APPLY MATCH THRESHOLD
    # --------------------------------------------------------

    best = best.filter(
        F.col("match_score") >= MATCH_THRESHOLD
    )

    # --------------------------------------------------------
    # SELECT OUTPUT COLUMNS
    # --------------------------------------------------------

    result = best.select(
        F.lit(source_label).alias("source"),

        F.col("source_entity_id"),
        F.col("source_business_name"),
        F.col("source_business_address"),
        F.col("source_country"),

        F.col("master_entity_id").alias(
            "matched_entity_id"
        ),
        F.col("master_business_name").alias(
            "matched_business_name"
        ),
        F.col("master_business_address").alias(
            "matched_business_address"
        ),

        F.col("name_similarity"),
        F.col("jaccard_similarity"),
        F.col("address_similarity"),
        F.col("match_score")
    )

    return result, candidate_pairs_out


# ============================================================
# RUN
# ============================================================

result2, candidates2 = match_source(
    source2,
    "SOURCE2"
)

result3, candidates3 = match_source(
    source3,
    "SOURCE3"
)


# ============================================================
# COMBINE
# ============================================================

final_result = result2.unionByName(result3)
all_candidates = candidates2.unionByName(candidates3)


# ============================================================
# OUTPUT HELPER
#
# Spark .csv() writes a directory of part-files.
# This helper consolidates them into a single TSV.
# ============================================================

def write_single_tsv(df, output_path):
    """Write a Spark DataFrame as a single TSV file."""

    tmp_dir = output_path + "_spark_tmp"

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("sep", "\t")
        .option("quote", '"')
        .option("escape", '"')
        .csv(tmp_dir)
    )

    # Find the single part file Spark produced
    part_files = glob.glob(
        os.path.join(tmp_dir, "part-*.csv")
    )

    if part_files:
        # Overwrite target with the part file
        if os.path.exists(output_path):
            os.remove(output_path)
        shutil.move(part_files[0], output_path)

    # Clean up temp directory
    shutil.rmtree(tmp_dir, ignore_errors=True)

    print("Wrote:", output_path)


# ============================================================
# WRITE OUTPUTS
# ============================================================

os.makedirs(args.output, exist_ok=True)

# --- matching_results.tsv ---

write_single_tsv(
    final_result,
    os.path.join(args.output, "matching_results.tsv")
)

# --- candidate_pairs.tsv ---

write_single_tsv(
    all_candidates,
    os.path.join(args.output, "candidate_pairs.tsv")
)


# ============================================================
# DONE
# ============================================================

print("\n==============================================")
print("MATCHING COMPLETE")
print("==============================================")
print("Output directory:", args.output)

spark.stop()
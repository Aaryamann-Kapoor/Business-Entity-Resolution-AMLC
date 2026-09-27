from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# ============================================================
# 1. START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BusinessEntityResolution")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .getOrCreate()
)

print("\n========================================")
print("Spark started successfully")
print("Spark version:", spark.version)
print("========================================\n")


# ============================================================
# 2. COMMON SCHEMA
# ============================================================

schema = StructType([
    StructField("entity_id", StringType(), True),
    StructField("business_name", StringType(), True),
    StructField("business_address", StringType(), True),
    StructField("country", StringType(), True)
])


# ============================================================
# 3. READ MASTER DATASET
# ============================================================

print("Reading MASTER dataset...")

master = (
    spark.read
    .schema(schema)
    .option("header", "false")
    .option("sep", "\t")
    .option("mode", "PERMISSIVE")
    .csv("data/master_clean.tsv")
)


# ============================================================
# 4. READ SOURCE 2
# ============================================================

print("Reading SOURCE 2...")

source2 = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .option("sep", "\t")
    .option("mode", "PERMISSIVE")
    .csv("data/train_source2.tsv")
)


# ============================================================
# 5. READ SOURCE 3
# ============================================================

print("Reading SOURCE 3...")

source3 = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .option("sep", "\t")
    .option("mode", "PERMISSIVE")
    .csv("data/train_source3.tsv")
)


# ============================================================
# 6. FUNCTION: NORMALIZE BUSINESS NAME
# ============================================================

def add_core_name(df):

    # --------------------------------------------------------
    # Convert name to uppercase
    # --------------------------------------------------------

    df = df.withColumn(
        "name_normalized",
        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("business_name"),
                    r"[^\p{L}\p{N}]+",
                    " "
                )
            )
        )
    )

    # --------------------------------------------------------
    # Remove repeated spaces
    # --------------------------------------------------------

    df = df.withColumn(
        "name_normalized",
        F.regexp_replace(
            F.col("name_normalized"),
            r"\s+",
            " "
        )
    )

    # --------------------------------------------------------
    # Remove legal suffixes from END of name
    # --------------------------------------------------------

    df = df.withColumn(
        "core_name",
        F.regexp_replace(
            F.col("name_normalized"),

            r"\s+"
            r"(PRIVATE\s+LIMITED|"
            r"PVT\s+LTD|"
            r"PVT\s+LIMITED|"
            r"LIMITED|"
            r"LTD|"
            r"INCORPORATED|"
            r"INC|"
            r"CORPORATION|"
            r"CORP|"
            r"COMPANY|"
            r"CO|"
            r"LLC)"
            r"\s*$",

            ""
        )
    )

    # --------------------------------------------------------
    # Final cleanup
    # --------------------------------------------------------

    df = df.withColumn(
        "core_name",
        F.trim(
            F.regexp_replace(
                F.col("core_name"),
                r"\s+",
                " "
            )
        )
    )

    return df


# ============================================================
# 7. FUNCTION: NORMALIZE ADDRESS + EXTRACT CITY
# ============================================================

def add_city(df):

    # --------------------------------------------------------
    # Normalize address
    # --------------------------------------------------------

    df = df.withColumn(
        "address_normalized",
        F.when(
            F.col("business_address").isNotNull(),

            F.upper(
                F.trim(
                    F.regexp_replace(
                        F.col("business_address"),
                        r"\s+",
                        " "
                    )
                )
            )
        )
    )

    # --------------------------------------------------------
    # US CITY EXTRACTION
    #
    # Typical US format:
    #
    # Street, CITY, STATE
    #
    # Example:
    #
    # 0200 Washington Street, ALVO, Nebraska
    #                         ↑
    #                        CITY
    #
    # 4828 Hedges Avenue, Kansas City, MO
    #                    ↑
    #                   CITY
    # --------------------------------------------------------

    df = df.withColumn(
        "city",
        F.when(
            F.upper(F.trim(F.col("country"))) == "US",

            F.trim(
                F.element_at(
                    F.split(
                        F.col("address_normalized"),
                        ","
                    ),
                    -2
                )
            )
        )
    )

    # --------------------------------------------------------
    # For now India city is left NULL.
    #
    # We will build a better India-specific parser next.
    # --------------------------------------------------------

    return df


# ============================================================
# 8. APPLY NAME NORMALIZATION
# ============================================================

print("Creating core names...")

master = add_core_name(master)

source2 = add_core_name(source2)

source3 = add_core_name(source3)


# ============================================================
# 9. APPLY CITY EXTRACTION
# ============================================================

print("Extracting cities...")

master = add_city(master)

source2 = add_city(source2)

source3 = add_city(source3)


# ============================================================
# 10. MASTER SAMPLE
# ============================================================

print("\n")
print("============================================================")
print("MASTER DATASET")
print("============================================================")

master.select(
    "entity_id",
    "business_name",
    "name_normalized",
    "core_name",
    "business_address",
    "city",
    "country"
).show(
    20,
    truncate=False
)


# ============================================================
# 11. SOURCE 2 SAMPLE
# ============================================================

print("\n")
print("============================================================")
print("SOURCE 2")
print("============================================================")

source2.select(
    "entity_id",
    "business_name",
    "name_normalized",
    "core_name",
    "business_address",
    "city",
    "country"
).show(
    20,
    truncate=False
)


# ============================================================
# 12. SOURCE 3 SAMPLE
# ============================================================

print("\n")
print("============================================================")
print("SOURCE 3")
print("============================================================")

source3.select(
    "entity_id",
    "business_name",
    "name_normalized",
    "core_name",
    "business_address",
    "city",
    "country"
).show(
    20,
    truncate=False
)


# ============================================================
# 13. BASIC COUNTS
# ============================================================

print("\n")
print("============================================================")
print("DATASET COUNTS")
print("============================================================")

print("MASTER :", master.count())

print("SOURCE2:", source2.count())

print("SOURCE3:", source3.count())


# ============================================================
# 14. CITY EXTRACTION CHECK
# ============================================================

print("\n")
print("============================================================")
print("US CITY EXTRACTION CHECK")
print("============================================================")

master.filter(
    F.col("country") == "US"
).select(
    "business_name",
    "business_address",
    "city"
).show(
    20,
    truncate=False
)


# ============================================================
# 15. STOP SPARK
# ============================================================

spark.stop()

print("\n========================================")
print("STEP 2 COMPLETE")
print("========================================")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# ============================================================
# CONFIGURATION
# ============================================================

MASTER_FILE = "data/master_clean.tsv"
SOURCE2_FILE = "data/train_source2.tsv"
SOURCE3_FILE = "data/train_source3.tsv"

MASTER_OUTPUT = "data/processed_master"
SOURCE2_OUTPUT = "data/processed_source2"
SOURCE3_OUTPUT = "data/processed_source3"


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BusinessEntityResolution")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print()
print("=" * 70)
print("BUSINESS ENTITY RESOLUTION")
print("=" * 70)
print("Spark version:", spark.version)
print("=" * 70)
print()


# ============================================================
# COMMON SCHEMA
# ============================================================

schema = StructType([
    StructField("entity_id", StringType(), True),
    StructField("business_name", StringType(), True),
    StructField("business_address", StringType(), True),
    StructField("country", StringType(), True)
])


# ============================================================
# READ MASTER
# ============================================================

print("Reading MASTER dataset...")

master = (
    spark.read
    .schema(schema)
    .option("header", "false")
    .option("sep", "\t")
    .option("mode", "PERMISSIVE")
    .csv(MASTER_FILE)
)


# ============================================================
# READ SOURCE 2
# ============================================================

print("Reading SOURCE 2...")

source2 = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .option("sep", "\t")
    .option("mode", "PERMISSIVE")
    .csv(SOURCE2_FILE)
)


# ============================================================
# READ SOURCE 3
# ============================================================

print("Reading SOURCE 3...")

source3 = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .option("sep", "\t")
    .option("mode", "PERMISSIVE")
    .csv(SOURCE3_FILE)
)


# ============================================================
# CLEAN BASIC COLUMNS
# ============================================================

def clean_basic_columns(df):

    return (
        df
        .withColumn(
            "entity_id",
            F.trim(F.col("entity_id"))
        )
        .withColumn(
            "business_name",
            F.trim(F.col("business_name"))
        )
        .withColumn(
            "business_address",
            F.trim(F.col("business_address"))
        )
        .withColumn(
            "country",
            F.trim(F.col("country"))
        )
    )


master = clean_basic_columns(master)
source2 = clean_basic_columns(source2)
source3 = clean_basic_columns(source3)


# ============================================================
# CORE NAME
# ============================================================

def add_core_name(df):

    # --------------------------------------------------------
    # Convert name to uppercase
    # Remove punctuation
    # Keep Unicode letters/numbers
    # --------------------------------------------------------

    df = df.withColumn(
        "name_normalized",
        F.upper(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("business_name"),
                        F.lit("")
                    ),
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
    # Remove legal suffixes
    # --------------------------------------------------------

    suffix_pattern = (
        r"\s+("
        r"PRIVATE\s+LIMITED|"
        r"PRIVATE\s+LTD|"
        r"PVT\s+LIMITED|"
        r"PVT\s+LTD|"
        r"LIMITED|"
        r"LTD|"
        r"INCORPORATED|"
        r"INC|"
        r"CORPORATION|"
        r"CORP|"
        r"COMPANY|"
        r"CO|"
        r"LLC|"
        r"LLP"
        r")\s*$"
    )

    df = df.withColumn(
        "core_name",
        F.regexp_replace(
            F.col("name_normalized"),
            suffix_pattern,
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
# ADDRESS NORMALIZATION
# ============================================================

def normalize_address(df):

    return df.withColumn(
        "address_normalized",
        F.upper(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col("business_address"),
                        F.lit("")
                    ),
                    r"\s+",
                    " "
                )
            )
        )
    )


# ============================================================
# CITY EXTRACTION
# ============================================================

def add_city(df):

    df = normalize_address(df)

    # ========================================================
    # US CITY
    #
    # Example:
    #
    # 0200 Washington Street, ALVO, Nebraska
    #
    # -> ALVO
    #
    # 4828 Hedges Avenue, Kansas City, MO
    #
    # -> KANSAS CITY
    # ========================================================

    us_city = F.trim(
        F.element_at(
            F.split(
                F.col("address_normalized"),
                ","
            ),
            -2
        )
    )

    df = df.withColumn(
        "city",
        F.when(
            F.upper(F.col("country")) == "US",
            us_city
        )
    )

    # ========================================================
    # COMMON INDIAN CITIES
    # ========================================================

    indian_cities = [
        "NEW DELHI",
        "DELHI",
        "MUMBAI",
        "KOLKATA",
        "CALCUTTA",
        "BENGALURU",
        "BANGALORE",
        "HYDERABAD",
        "CHENNAI",
        "MADRAS",
        "PUNE",
        "AHMEDABAD",
        "SURAT",
        "JAIPUR",
        "LUCKNOW",
        "KANPUR",
        "NAGPUR",
        "INDORE",
        "BHOPAL",
        "PATNA",
        "VADODARA",
        "BARODA",
        "COIMBATORE",
        "KOCHI",
        "CHANDIGARH",
        "BHUBANESWAR",
        "GUWAHATI",
        "VISAKHAPATNAM",
        "VIJAYAWADA",
        "MYSORE",
        "MYSURU",
        "THANE",
        "NASHIK",
        "RAJKOT",
        "LUDHIANA",
        "AMRITSAR",
        "DEHRADUN",
        "NOIDA",
        "GURGAON",
        "GURUGRAM",
        "FARIDABAD",
        "GHAZIABAD",
        "RANCHI",
        "KOLHAPUR",
        "JALANDHAR",
        "AGRA",
        "VARANASI",
        "PRAYAGRAJ",
        "ALLAHABAD",
        "MEERUT",
        "KOZHIKODE",
        "THIRUVANANTHAPURAM",
        "MANGALORE",
        "MANGALURU",
        "TIRUPPUR",
        "SALEM",
        "MADURAI",
        "JODHPUR",
        "UDAIPUR",
        "AJMER",
        "KOTA"
    ]

    # Escape city names for regex
    city_pattern_values = [
        x.replace(" ", r"\s+")
        for x in indian_cities
    ]

    city_pattern = (
        r"(?i)(?:^|,\s*)("
        + "|".join(city_pattern_values)
        + r")(?:\s*,|$)"
    )

    india_city = F.regexp_extract(
        F.col("address_normalized"),
        city_pattern,
        1
    )

    # --------------------------------------------------------
    # Apply India city
    # --------------------------------------------------------

    df = df.withColumn(
        "city",
        F.when(
            F.upper(F.col("country")) == "INDIA",
            india_city
        ).otherwise(F.col("city"))
    )

    return df


# ============================================================
# CITY NORMALIZATION
# ============================================================

def normalize_city(df):

    return (
        df
        .withColumn(
            "city",
            F.upper(
                F.trim(
                    F.regexp_replace(
                        F.coalesce(
                            F.col("city"),
                            F.lit("")
                        ),
                        r"[^A-Z0-9]+",
                        " "
                    )
                )
            )
        )
        .withColumn(
            "city",
            F.regexp_replace(
                F.col("city"),
                r"\s+",
                " "
            )
        )
    )


# ============================================================
# BLOCK KEY
# ============================================================

def add_block_key(df):

    # --------------------------------------------------------
    # Country normalized
    # --------------------------------------------------------

    df = df.withColumn(
        "country_normalized",
        F.upper(
            F.trim(
                F.col("country")
            )
        )
    )

    # --------------------------------------------------------
    # Create block key
    #
    # CORE NAME + CITY + COUNTRY
    # --------------------------------------------------------

    df = df.withColumn(
        "block_key",
        F.concat_ws(
            "|",
            F.col("core_name"),
            F.col("city"),
            F.col("country_normalized")
        )
    )

    return df


# ============================================================
# PROCESS MASTER
# ============================================================

print()
print("Processing MASTER...")

master = add_core_name(master)
master = add_city(master)
master = normalize_city(master)
master = add_block_key(master)


# ============================================================
# PROCESS SOURCE 2
# ============================================================

print("Processing SOURCE 2...")

source2 = add_core_name(source2)
source2 = add_city(source2)
source2 = normalize_city(source2)
source2 = add_block_key(source2)


# ============================================================
# PROCESS SOURCE 3
# ============================================================

print("Processing SOURCE 3...")

source3 = add_core_name(source3)
source3 = add_city(source3)
source3 = normalize_city(source3)
source3 = add_block_key(source3)


# ============================================================
# MASTER SAMPLE
# ============================================================

print()
print("=" * 70)
print("MASTER SAMPLE")
print("=" * 70)

master.select(
    "entity_id",
    "business_name",
    "core_name",
    "business_address",
    "city",
    "country",
    "block_key"
).show(
    20,
    truncate=False
)


# ============================================================
# SOURCE 2 SAMPLE
# ============================================================

print()
print("=" * 70)
print("SOURCE 2 SAMPLE")
print("=" * 70)

source2.select(
    "entity_id",
    "business_name",
    "core_name",
    "business_address",
    "city",
    "country",
    "block_key"
).show(
    20,
    truncate=False
)


# ============================================================
# SOURCE 3 SAMPLE
# ============================================================

print()
print("=" * 70)
print("SOURCE 3 SAMPLE")
print("=" * 70)

source3.select(
    "entity_id",
    "business_name",
    "core_name",
    "business_address",
    "city",
    "country",
    "block_key"
).show(
    20,
    truncate=False
)


# ============================================================
# BASIC STATISTICS
# ============================================================

print()
print("=" * 70)
print("BASIC COUNTS")
print("=" * 70)

print("MASTER records :", master.count())
print("SOURCE 2 records:", source2.count())
print("SOURCE 3 records:", source3.count())


# ============================================================
# BLOCK STATISTICS
# ============================================================

print()
print("=" * 70)
print("MASTER BLOCK STATISTICS")
print("=" * 70)

master.groupBy(
    "block_key"
).count() \
 .orderBy(
     F.desc("count")
 ) \
 .show(
     20,
     truncate=False
)


# ============================================================
# SAVE MASTER
# ============================================================

print()
print("Saving processed MASTER...")

(
    master
    .write
    .mode("overwrite")
    .parquet(MASTER_OUTPUT)
)


# ============================================================
# SAVE SOURCE 2
# ============================================================

print("Saving processed SOURCE 2...")

(
    source2
    .write
    .mode("overwrite")
    .parquet(SOURCE2_OUTPUT)
)


# ============================================================
# SAVE SOURCE 3
# ============================================================

print("Saving processed SOURCE 3...")

(
    source3
    .write
    .mode("overwrite")
    .parquet(SOURCE3_OUTPUT)
)


# ============================================================
# FINAL MESSAGE
# ============================================================

print()
print("=" * 70)
print("SEGREGATION COMPLETE")
print("=" * 70)

print()
print("Generated:")
print("  data/processed_master")
print("  data/processed_source2")
print("  data/processed_source3")
print()
print("Blocking key:")
print("  CORE_NAME + CITY + COUNTRY")
print()


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()
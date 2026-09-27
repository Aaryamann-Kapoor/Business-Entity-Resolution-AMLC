from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("ProcessSource2")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


INPUT = "data/train_source2.tsv"
OUTPUT = "data/processed_source2"


# ============================================================
# READ SOURCE 2
# ============================================================

print("Reading Source 2...")

df = (
    spark.read
    .option("header", "true")
    .option("sep", "\t")
    .option("mode", "PERMISSIVE")
    .csv(INPUT)
)


# ============================================================
# CHECK COLUMNS
# ============================================================

print("\nColumns:")
print(df.columns)

print("\nSchema:")
df.printSchema()


# ============================================================
# CLEAN BASIC COLUMNS
# ============================================================

df = (
    df
    .withColumn("entity_id", F.trim(F.col("entity_id")))
    .withColumn("business_name", F.trim(F.col("business_name")))
    .withColumn("business_address", F.trim(F.col("business_address")))
    .withColumn("country", F.trim(F.col("country")))
)


# ============================================================
# NORMALIZED NAME
# ============================================================

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

df = df.withColumn(
    "name_normalized",
    F.regexp_replace(
        F.col("name_normalized"),
        r"\s+",
        " "
    )
)


# ============================================================
# CORE NAME
# ============================================================

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


# ============================================================
# ADDRESS NORMALIZATION
# ============================================================

df = df.withColumn(
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
# US CITY
# ============================================================

us_city = F.when(
    F.size(F.split(F.col("address_normalized"), ",")) >= 2,
    F.trim(
        F.element_at(
            F.split(F.col("address_normalized"), ","),
            -2
        )
    )
).otherwise(F.lit(""))

df = df.withColumn(
    "city",
    F.when(
        F.upper(F.col("country")) == "US",
        us_city
    )
)


# ============================================================
# INDIA CITY
# ============================================================

indian_cities = [
    "NEW DELHI",
    "DELHI",
    "MUMBAI",
    "KOLKATA",
    "BENGALURU",
    "BANGALORE",
    "HYDERABAD",
    "CHENNAI",
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
    "MANGALORE",
    "MANGALURU",
    "MADURAI",
    "JODHPUR",
    "UDAIPUR",
    "AJMER",
    "KOTA"
]

city_pattern = (
    r"(?i)(?:^|,\s*)("
    + "|".join(
        x.replace(" ", r"\s+")
        for x in indian_cities
    )
    + r")(?:\s*,|$)"
)

india_city = F.regexp_extract(
    F.col("address_normalized"),
    city_pattern,
    1
)

df = df.withColumn(
    "city",
    F.when(
        F.upper(F.col("country")) == "INDIA",
        india_city
    ).otherwise(F.col("city"))
)


# ============================================================
# NORMALIZE CITY
# ============================================================

df = df.withColumn(
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

df = df.withColumn(
    "city",
    F.regexp_replace(
        F.col("city"),
        r"\s+",
        " "
    )
)


# ============================================================
# COUNTRY NORMALIZATION
# ============================================================

df = df.withColumn(
    "country_normalized",
    F.upper(
        F.trim(
            F.col("country")
        )
    )
)


# ============================================================
# BLOCK KEY
# ============================================================

df = df.withColumn(
    "block_key",
    F.concat_ws(
        "|",
        F.col("core_name"),
        F.col("city"),
        F.col("country_normalized")
    )
)


# ============================================================
# SHOW SAMPLE
# ============================================================

print("\n================ SOURCE 2 SAMPLE ================\n")

df.select(
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
# WRITE PARQUET
# ============================================================

print("\nWriting Source 2 Parquet...")

(
    df
    .write
    .mode("overwrite")
    .parquet(OUTPUT)
)


print("\n========================================")
print("SOURCE 2 COMPLETE")
print("========================================")


spark.stop()
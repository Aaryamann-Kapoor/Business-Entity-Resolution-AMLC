import csv
import os
import re


# ============================================================
# FILES
# ============================================================

MASTER_FILE = "data/train_source1.tsv"
SOURCE2_FILE = "data/train_source2.tsv"
SOURCE3_FILE = "data/train_source3.tsv"

OUTPUT_DIR = "data/final_results"

MATCHED_FILE = os.path.join(
    OUTPUT_DIR,
    "matched.tsv"
)

UNMATCHED_FILE = os.path.join(
    OUTPUT_DIR,
    "unmatched.tsv"
)


# ============================================================
# NORMALIZATION
# ============================================================

def normalize(value):

    if value is None:
        return ""

    value = value.upper().strip()

    value = re.sub(
        r"[^\w\s]",
        " ",
        value
    )

    value = re.sub(
        r"\s+",
        " ",
        value
    )

    return value.strip()


# ============================================================
# CORE NAME
# ============================================================

def get_core_name(name):

    name = normalize(name)

    suffix_pattern = (
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
        r"\s*$"
    )

    name = re.sub(
        suffix_pattern,
        "",
        name
    )

    return name.strip()


# ============================================================
# CITY EXTRACTION
# ============================================================

def get_city(address, country):

    if not address:
        return ""

    address = " ".join(
        address.strip().upper().split()
    )

    parts = [
        p.strip()
        for p in address.split(",")
        if p.strip()
    ]

    country = normalize(country)

    # --------------------------------------------------------
    # UNITED STATES
    # --------------------------------------------------------

    if country == "US":

        if len(parts) >= 3:

            # Example:
            #
            # 18900 HARL WEILLER ROAD,
            # CALDWELL,
            # OHIO
            #
            # city = CALDWELL

            return normalize(parts[-2])

        return ""

    # --------------------------------------------------------
    # INDIA
    # --------------------------------------------------------
    #
    # India addresses are less standardized.
    #
    # For now we look for common city names present
    # in the supplied data.
    # --------------------------------------------------------

    india_cities = [
        "DELHI",
        "NEW DELHI",
        "MUMBAI",
        "BOMBAY",
        "GURGAON",
        "GURUGRAM",
        "NOIDA",
        "FARIDABAD",
        "BENGALURU",
        "BANGALORE",
        "HYDERABAD",
        "CHENNAI",
        "KOLKATA",
        "PUNE",
        "AHMEDABAD",
        "JAIPUR",
        "LUCKNOW",
        "CHANDIGARH",
        "INDORE",
        "BHOPAL",
        "SURAT",
        "NAGPUR",
        "PATNA",
        "KOCHI",
        "COIMBATORE",
        "VISAKHAPATNAM",
        "VADODARA",
        "RAJKOT",
        "NASHIK",
        "THANE",
        "VIJAYAWADA",
        "RANCHI",
        "DEHRADUN",
        "GUWAHATI"
    ]

    for city in india_cities:

        if city in address:
            return city

    return ""


# ============================================================
# CREATE KEY
# ============================================================

def create_key(
    core_name,
    city,
    country
):

    return (
        normalize(core_name),
        normalize(city),
        normalize(country)
    )


# ============================================================
# READ MASTER
# ============================================================

def load_master():

    print()
    print("Loading MASTER...")

    master_keys = {}

    with open(
        MASTER_FILE,
        "r",
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as f:

        reader = csv.reader(
            f,
            delimiter="\t"
        )

        count = 0

        for row in reader:

            if len(row) < 4:
                continue

            entity_id = row[0]
            business_name = row[1]
            address = row[2]
            country = row[3]

            core_name = get_core_name(
                business_name
            )

            city = get_city(
                address,
                country
            )

            key = create_key(
                core_name,
                city,
                country
            )

            if key not in master_keys:

                master_keys[key] = []

            master_keys[key].append(
                entity_id
            )

            count += 1

            if count % 500000 == 0:

                print(
                    "Master records:",
                    f"{count:,}"
                )

    print(
        "Master loaded:",
        f"{count:,}"
    )

    print(
        "Unique keys:",
        f"{len(master_keys):,}"
    )

    return master_keys


# ============================================================
# PROCESS SOURCE
# ============================================================

def process_source(
    source_file,
    source_name,
    master_keys,
    matched_writer,
    unmatched_writer
):

    print()
    print("=" * 60)
    print("Processing", source_name)
    print("=" * 60)

    matched = 0
    unmatched = 0
    total = 0

    with open(
        source_file,
        "r",
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as f:

        reader = csv.reader(
            f,
            delimiter="\t"
        )

        for row in reader:

            if len(row) < 4:
                continue

            entity_id = row[0]
            business_name = row[1]
            address = row[2]
            country = row[3]

            core_name = get_core_name(
                business_name
            )

            city = get_city(
                address,
                country
            )

            key = create_key(
                core_name,
                city,
                country
            )

            master_entities = master_keys.get(
                key
            )

            # ------------------------------------------------
            # MATCH
            # ------------------------------------------------

            if master_entities:

                for master_id in master_entities:

                    matched_writer.writerow([
                        source_name,
                        entity_id,
                        business_name,
                        address,
                        country,
                        core_name,
                        city,
                        master_id
                    ])

                matched += 1

            # ------------------------------------------------
            # UNMATCHED
            # ------------------------------------------------

            else:

                unmatched_writer.writerow([
                    source_name,
                    entity_id,
                    business_name,
                    address,
                    country,
                    core_name,
                    city
                ])

                unmatched += 1

            total += 1

            if total % 500000 == 0:

                print(
                    f"{source_name}: "
                    f"{total:,} processed | "
                    f"{matched:,} matched | "
                    f"{unmatched:,} unmatched"
                )

    print()
    print(
        source_name,
        "complete"
    )

    print(
        "Total:",
        f"{total:,}"
    )

    print(
        "Matched:",
        f"{matched:,}"
    )

    print(
        "Unmatched:",
        f"{unmatched:,}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Delete old outputs
    # --------------------------------------------------------

    if os.path.exists(MATCHED_FILE):
        os.remove(MATCHED_FILE)

    if os.path.exists(UNMATCHED_FILE):
        os.remove(UNMATCHED_FILE)

    # --------------------------------------------------------
    # MASTER
    # --------------------------------------------------------

    master_keys = load_master()

    # --------------------------------------------------------
    # Output writers
    # --------------------------------------------------------

    matched_header = [
        "source",
        "source_entity_id",
        "source_business_name",
        "source_business_address",
        "source_country",
        "source_core_name",
        "source_city",
        "master_entity_id"
    ]

    unmatched_header = [
        "source",
        "source_entity_id",
        "source_business_name",
        "source_business_address",
        "source_country",
        "source_core_name",
        "source_city"
    ]

    with open(
        MATCHED_FILE,
        "w",
        encoding="utf-8",
        newline=""
    ) as matched_file, open(
        UNMATCHED_FILE,
        "w",
        encoding="utf-8",
        newline=""
    ) as unmatched_file:

        matched_writer = csv.writer(
            matched_file,
            delimiter="\t"
        )

        unmatched_writer = csv.writer(
            unmatched_file,
            delimiter="\t"
        )

        matched_writer.writerow(
            matched_header
        )

        unmatched_writer.writerow(
            unmatched_header
        )

        # ----------------------------------------------------
        # SOURCE 2
        # ----------------------------------------------------

        process_source(
            SOURCE2_FILE,
            "SOURCE2",
            master_keys,
            matched_writer,
            unmatched_writer
        )

        # ----------------------------------------------------
        # SOURCE 3
        # ----------------------------------------------------

        process_source(
            SOURCE3_FILE,
            "SOURCE3",
            master_keys,
            matched_writer,
            unmatched_writer
        )

    print()
    print("=" * 60)
    print("FINISHED")
    print("=" * 60)

    print()
    print("Matched:")
    print(MATCHED_FILE)

    print()
    print("Unmatched:")
    print(UNMATCHED_FILE)


if __name__ == "__main__":
    main()
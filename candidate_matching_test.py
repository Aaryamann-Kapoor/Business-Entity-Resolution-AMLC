import csv
import os
import re
import unicodedata
from collections import defaultdict, Counter


# ============================================================
# CONFIGURATION
# ============================================================

MASTER_FILE = "test_source1.tsv"
SOURCE2_FILE = "/tmp/test_source2_10k.tsv"
SOURCE3_FILE = "test_source3.tsv"
OUTPUT_DIR = "data/test_candidates"

SOURCE2_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "source2_candidates.tsv"
)

SOURCE3_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "source3_candidates.tsv"
)

# Prevent very common names from producing enormous blocks.
MAX_CANDIDATES_PER_BLOCK = 100


# ============================================================
# US STATE CODES
# ============================================================

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE",
    "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY"
}


# ============================================================
# LEGAL SUFFIXES
# ============================================================

LEGAL_SUFFIXES = [
    "PRIVATE LIMITED",
    "PVT LIMITED",
    "PVT LTD",
    "PRIVATE LTD",
    "LIMITED",
    "LTD",
    "INCORPORATED",
    "INC",
    "CORPORATION",
    "CORP",
    "COMPANY",
    "CO",
    "LLC",
    "LLP"
]


# ============================================================
# NORMALIZE TEXT
# ============================================================

def normalize_text(value):

    if not value:
        return ""

    value = unicodedata.normalize(
        "NFKC",
        str(value)
    )

    value = value.upper().strip()

    # Keep Unicode letters/numbers.
    value = re.sub(
        r"[^\w\s]",
        " ",
        value,
        flags=re.UNICODE
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

    name = normalize_text(name)

    if not name:
        return ""

    # Remove legal suffixes repeatedly.
    changed = True

    while changed:

        changed = False

        for suffix in LEGAL_SUFFIXES:

            pattern = (
                r"\s+"
                + re.escape(suffix)
                + r"$"
            )

            new_name = re.sub(
                pattern,
                "",
                name
            ).strip()

            if new_name != name:

                name = new_name
                changed = True

                break

    return name


# ============================================================
# CITY EXTRACTION - US
# ============================================================

def get_us_city(address):

    if not address:
        return ""

    address = normalize_text(address)

    parts = [
        p.strip()
        for p in address.split(",")
        if p.strip()
    ]

    if len(parts) < 2:
        return ""

    # --------------------------------------------------------
    # Find the US state.
    #
    # Examples:
    #
    # 4828 HEDGES AVENUE, KANSAS CITY, MO
    #
    # GREENSBORO, NC, 19 1/2 STARDUST TRAIL
    # --------------------------------------------------------

    state_index = None

    for i, part in enumerate(parts):

        if part in US_STATES:

            state_index = i
            break

    if state_index is None:
        return ""

    # --------------------------------------------------------
    # State at the end:
    #
    # STREET, CITY, STATE
    # --------------------------------------------------------

    if state_index == len(parts) - 1:

        if state_index >= 1:

            return parts[state_index - 1]

        return ""

    # --------------------------------------------------------
    # State followed by street:
    #
    # CITY, STATE, STREET
    # --------------------------------------------------------

    if state_index + 1 < len(parts):

        return parts[state_index - 1] if state_index >= 1 else parts[0]

    return ""


# ============================================================
# CITY EXTRACTION - INDIA
# ============================================================

def get_india_city(address):

    if not address:
        return ""

    address = normalize_text(address)

    parts = [
        p.strip()
        for p in address.split(",")
        if p.strip()
    ]

    if not parts:
        return ""

    # --------------------------------------------------------
    # Indian addresses are much less standardized.
    #
    # We use known city/state names where possible.
    # --------------------------------------------------------

    known_cities = [
        "DELHI",
        "NEW DELHI",
        "MUMBAI",
        "BENGALURU",
        "BANGALORE",
        "HYDERABAD",
        "CHENNAI",
        "KOLKATA",
        "PUNE",
        "AHMEDABAD",
        "JAIPUR",
        "LUCKNOW",
        "BHOPAL",
        "CHANDIGARH",
        "GURGAON",
        "GURUGRAM",
        "NOIDA",
        "FARIDABAD",
        "INDORE",
        "SURAT",
        "NAGPUR",
        "PATNA",
        "KANPUR",
        "VARANASI",
        "KOCHI",
        "COIMBATORE",
        "VISAKHAPATNAM",
        "VIJAYAWADA",
        "THANE",
        "NASHIK",
        "RAJKOT",
        "AMRITSAR",
        "LUDHIANA",
        "KANNUR",
        "MYSORE",
        "MYSURU"
    ]

    # Search from the end first.
    for part in reversed(parts):

        if part in known_cities:

            return part

    # --------------------------------------------------------
    # If city isn't recognized, use the final meaningful
    # component before the state when possible.
    # --------------------------------------------------------

    indian_states = [
        "DELHI",
        "MAHARASHTRA",
        "TELANGANA",
        "KARNATAKA",
        "TAMIL NADU",
        "WEST BENGAL",
        "GUJARAT",
        "RAJASTHAN",
        "MADHYA PRADESH",
        "UTTAR PRADESH",
        "PUNJAB",
        "HARYANA",
        "KERALA",
        "BIHAR",
        "ODISHA",
        "ANDHRA PRADESH",
        "JHARKHAND",
        "CHHATTISGARH",
        "UTTARAKHAND",
        "GOA",
        "ASSAM"
    ]

    for i, part in enumerate(parts):

        if part in indian_states:

            if i > 0:
                return parts[i - 1]

    return ""


# ============================================================
# CITY
# ============================================================

def get_city(address, country):

    country = normalize_text(country)

    if country == "US":

        return get_us_city(address)

    if country == "INDIA":

        return get_india_city(address)

    return ""


# ============================================================
# BLOCK KEY
# ============================================================

def get_block_key(
    business_name,
    business_address,
    country
):

    country_key = normalize_text(country)

    city_key = get_city(
        business_address,
        country
    )

    core_name = get_core_name(
        business_name
    )

    return (
        country_key,
        city_key,
        core_name
    )


# ============================================================
# READ MASTER
# ============================================================

def build_master_index():

    print()
    print("=" * 70)
    print("BUILDING MASTER INDEX")
    print("=" * 70)
    print()

    master_index = defaultdict(list)

    total = 0
    skipped = 0

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

        for row in reader:

            # ------------------------------------------------
            # Source 1 contains one malformed first row.
            # ------------------------------------------------

            if len(row) != 4:

                skipped += 1
                continue

            entity_id = row[0].strip()
            business_name = row[1].strip()
            business_address = row[2].strip()
            country = row[3].strip()

            if not entity_id:
                continue

            country_key, city_key, core_name = get_block_key(
                business_name,
                business_address,
                country
            )

            if not core_name:
                continue

            key = (
                country_key,
                city_key,
                core_name
            )

            master_index[key].append({

                "entity_id": entity_id,

                "business_name": business_name,

                "business_address": business_address,

                "country": country,

                "city": city_key,

                "core_name": core_name

            })

            total += 1

            if total % 100000 == 0:

                print(
                    f"Master indexed: {total:,}"
                )

    print()
    print(
        f"Master records indexed: {total:,}"
    )

    print(
        f"Malformed rows skipped: {skipped:,}"
    )

    print(
        f"Blocking keys: {len(master_index):,}"
    )

    return master_index


# ============================================================
# GENERATE CANDIDATES
# ============================================================

def generate_candidates(
    master_index,
    source_file,
    output_file,
    source_name
):

    print()
    print("=" * 70)
    print(
        f"PROCESSING {source_name}"
    )
    print("=" * 70)
    print()

    total = 0
    candidate_rows = 0
    no_candidates = 0
    oversized_blocks = 0
    malformed = 0

    # Statistics for diagnostics.
    candidate_counts = Counter()

    with open(
        source_file,
        "r",
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as source:

        reader = csv.reader(
            source,
            delimiter="\t"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8",
            newline=""
        ) as output:

            writer = csv.writer(
                output,
                delimiter="\t"
            )

            writer.writerow([

                "source",

                "source_entity_id",

                "source_business_name",

                "source_business_address",

                "source_country",

                "source_core_name",

                "source_city",

                "candidate_entity_id",

                "candidate_business_name",

                "candidate_business_address",

                "candidate_country",

                "candidate_core_name",

                "candidate_city",

                "block_type"

            ])

            for row in reader:

                if len(row) != 4:

                    malformed += 1
                    continue

                entity_id = row[0].strip()
                business_name = row[1].strip()
                business_address = row[2].strip()
                country = row[3].strip()

                if not entity_id:
                    continue

                total += 1

                country_key, city_key, core_name = get_block_key(
                    business_name,
                    business_address,
                    country
                )

                if not core_name:

                    no_candidates += 1
                    continue

                key = (
                    country_key,
                    city_key,
                    core_name
                )

                candidates = master_index.get(
                    key,
                    []
                )

                candidate_count = len(candidates)

                candidate_counts[
                    candidate_count
                ] += 1

                # ------------------------------------------------
                # Protect against huge common-name blocks.
                # ------------------------------------------------

                if candidate_count > MAX_CANDIDATES_PER_BLOCK:

                    oversized_blocks += 1

                    no_candidates += 1

                    continue

                if candidate_count == 0:

                    no_candidates += 1

                    continue

                for candidate in candidates:

                    writer.writerow([

                        source_name,

                        entity_id,

                        business_name,

                        business_address,

                        country,

                        core_name,

                        city_key,

                        candidate["entity_id"],

                        candidate["business_name"],

                        candidate["business_address"],

                        candidate["country"],

                        candidate["core_name"],

                        candidate["city"],

                        "COUNTRY_CITY_CORE_NAME"

                    ])

                    candidate_rows += 1

                if total % 100000 == 0:

                    print(

                        f"{source_name}: "

                        f"{total:,} records | "

                        f"{candidate_rows:,} candidates | "

                        f"{no_candidates:,} no candidates"

                    )

    print()
    print(
        f"{source_name} COMPLETE"
    )

    print(
        f"Source records:       {total:,}"
    )

    print(
        f"Candidate rows:       {candidate_rows:,}"
    )

    print(
        f"No candidates:        {no_candidates:,}"
    )

    print(
        f"Oversized blocks:     {oversized_blocks:,}"
    )

    print(
        f"Malformed rows:       {malformed:,}"
    )

    print(
        f"Output:               {output_file}"
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    master_index = build_master_index()

    generate_candidates(
        master_index,
        SOURCE2_FILE,
        SOURCE2_OUTPUT,
        "SOURCE2"
    )

    generate_candidates(
        master_index,
        SOURCE3_FILE,
        SOURCE3_OUTPUT,
        "SOURCE3"
    )

    print()
    print("=" * 70)
    print("CANDIDATE GENERATION FINISHED")
    print("=" * 70)
    print()

    print(
        f"Source 2 candidates: {SOURCE2_OUTPUT}"
    )

    print(
        f"Source 3 candidates: {SOURCE3_OUTPUT}"
    )

    print()
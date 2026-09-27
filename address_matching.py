import csv
import re
from difflib import SequenceMatcher


SOURCE_FILES = [
    "data/candidates/source2_candidates.tsv",
    "data/candidates/source3_candidates.tsv",
]

OUTPUT_FILES = {
    "source2": "data/address_scored_source2.tsv",
    "source3": "data/address_scored_source3.tsv",
}


# ============================================================
# NORMALIZATION
# ============================================================

ABBREVIATIONS = {
    "ST": "STREET",
    "RD": "ROAD",
    "AVE": "AVENUE",
    "AV": "AVENUE",
    "BLVD": "BOULEVARD",
    "DR": "DRIVE",
    "LN": "LANE",
    "HWY": "HIGHWAY",
    "PKWY": "PARKWAY",
    "CTR": "CENTER",
    "CT": "COURT",
    "PL": "PLACE",
    "STE": "SUITE",
    "APT": "APARTMENT",
    "FL": "FLOOR",
    "BLDG": "BUILDING",
    "NO": "NUMBER",
}


RELATION_WORDS = {
    "NEAR",
    "NEARBY",
    "OPPOSITE",
    "OPP",
    "BESIDE",
    "NEXT",
    "BEHIND",
    "FRONT",
    "ADJACENT",
    "CLOSE",
    "NEARER",
}


def normalize_text(value):

    if not value:
        return ""

    value = value.upper()

    value = re.sub(r"[#.,;:/()\-]", " ", value)

    value = re.sub(r"\s+", " ", value)

    words = []

    for word in value.split():

        word = ABBREVIATIONS.get(word, word)

        words.append(word)

    return " ".join(words).strip()


# ============================================================
# NUMBER EXTRACTION
# ============================================================

def extract_numbers(address):

    address = normalize_text(address)

    return set(re.findall(r"\b\d+[A-Z]?\b", address))


# ============================================================
# CITY EXTRACTION
# ============================================================

def extract_city(address):

    address = normalize_text(address)

    parts = [
        p.strip()
        for p in address.split(",")
        if p.strip()
    ]

    if len(parts) >= 2:

        return parts[-2]

    return ""


# ============================================================
# LANDMARK / LOCATION TOKENS
# ============================================================

def extract_location_tokens(address):

    address = normalize_text(address)

    words = address.split()

    tokens = set()

    for word in words:

        if len(word) >= 4 and word not in RELATION_WORDS:
            tokens.add(word)

    return tokens


# ============================================================
# TOKEN SIMILARITY
# ============================================================

def token_similarity(a, b):

    a_tokens = set(normalize_text(a).split())
    b_tokens = set(normalize_text(b).split())

    if not a_tokens or not b_tokens:
        return 0.0

    intersection = len(a_tokens & b_tokens)

    union = len(a_tokens | b_tokens)

    return intersection / union


# ============================================================
# CHARACTER SIMILARITY
# ============================================================

def character_similarity(a, b):

    a = normalize_text(a)
    b = normalize_text(b)

    if not a or not b:
        return 0.0

    return SequenceMatcher(None, a, b).ratio()


# ============================================================
# NUMBER SIMILARITY
# ============================================================

def number_similarity(a, b):

    numbers_a = extract_numbers(a)
    numbers_b = extract_numbers(b)

    if not numbers_a or not numbers_b:

        # No number information.
        # Don't automatically penalize.
        return 0.5

    if numbers_a & numbers_b:

        return 1.0

    return 0.0


# ============================================================
# LOCATION / LANDMARK SIMILARITY
# ============================================================

def location_similarity(a, b):

    tokens_a = extract_location_tokens(a)
    tokens_b = extract_location_tokens(b)

    if not tokens_a or not tokens_b:
        return 0.0

    intersection = tokens_a & tokens_b

    if not intersection:
        return 0.0

    return len(intersection) / min(
        len(tokens_a),
        len(tokens_b)
    )


# ============================================================
# ADDRESS SCORE
# ============================================================

def address_score(source_address, candidate_address):

    source = normalize_text(source_address)
    candidate = normalize_text(candidate_address)

    if not source or not candidate:

        return 0.0, 0.0, 0.0, 0.0

    token_score = token_similarity(
        source,
        candidate
    )

    char_score = character_similarity(
        source,
        candidate
    )

    number_score = number_similarity(
        source,
        candidate
    )

    location_score = location_similarity(
        source,
        candidate
    )

    # Main address score
    score = (
        0.35 * token_score
        + 0.25 * char_score
        + 0.25 * number_score
        + 0.15 * location_score
    )

    return (
        score,
        token_score,
        char_score,
        number_score
    )


# ============================================================
# PROCESS SOURCE
# ============================================================

def process_source(input_file, output_file):

    print()
    print("=" * 70)
    print("PROCESSING")
    print(input_file)
    print("=" * 70)

    total = 0

    with open(
        input_file,
        "r",
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as infile, open(
        output_file,
        "w",
        encoding="utf-8",
        newline=""
    ) as outfile:

        reader = csv.DictReader(
            infile,
            delimiter="\t"
        )

        fieldnames = reader.fieldnames + [
            "address_score",
            "token_score",
            "character_score",
            "number_score",
            "decision",
        ]

        writer = csv.DictWriter(
            outfile,
            fieldnames=fieldnames,
            delimiter="\t"
        )

        writer.writeheader()

        for row in reader:

            total += 1

            source_address = row.get(
                "source_business_address",
                ""
            )

            candidate_address = row.get(
                "candidate_business_address",
                ""
            )

            score, token_score, char_score, number_score = (
                address_score(
                    source_address,
                    candidate_address
                )
            )

            if score >= 0.85:

                decision = "MATCH"

            elif score >= 0.60:

                decision = "REVIEW"

            else:

                decision = "UNMATCHED"

            row["address_score"] = f"{score:.4f}"

            row["token_score"] = f"{token_score:.4f}"

            row["character_score"] = f"{char_score:.4f}"

            row["number_score"] = f"{number_score:.4f}"

            row["decision"] = decision

            writer.writerow(row)

            if total % 100000 == 0:

                print(
                    f"Processed: {total:,}"
                )

    print()
    print(f"Finished: {total:,} candidates")
    print(f"Output: {output_file}")


# ============================================================
# MAIN
# ============================================================

process_source(
    SOURCE_FILES[0],
    OUTPUT_FILES["source2"]
)

process_source(
    SOURCE_FILES[1],
    OUTPUT_FILES["source3"]
)

print()
print("=" * 70)
print("ADDRESS MATCHING COMPLETE")
print("=" * 70)

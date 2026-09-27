import csv
import re

MASTER_FILE = "data/train_source1.tsv"
SOURCE2_FILE = "data/train_source2.tsv"
SOURCE3_FILE = "data/train_source3.tsv"
GROUND_TRUTH = "train_ground_truth.tsv"


def normalize(value):
    if not value:
        return ""

    value = value.upper().strip()
    value = re.sub(r"[^\w\s]", " ", value)
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def core_name(name):
    name = normalize(name)

    pattern = (
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

    return re.sub(pattern, "", name).strip()


def city_from_address(address, country):
    address = normalize(address)
    country = normalize(country)

    if not address:
        return ""

    parts = [
        x.strip()
        for x in address.split(",")
        if x.strip()
    ]

    if country == "US" and len(parts) >= 3:
        return normalize(parts[-2])

    indian_cities = [
        "DELHI", "NEW DELHI", "MUMBAI", "BOMBAY",
        "GURGAON", "GURUGRAM", "NOIDA", "FARIDABAD",
        "BENGALURU", "BANGALORE", "HYDERABAD",
        "CHENNAI", "KOLKATA", "PUNE", "AHMEDABAD",
        "JAIPUR", "LUCKNOW", "CHANDIGARH", "INDORE",
        "BHOPAL", "SURAT", "NAGPUR", "PATNA",
        "KOCHI", "COIMBATORE", "VISAKHAPATNAM",
        "VADODARA", "RAJKOT", "NASHIK", "THANE",
        "VIJAYAWADA", "RANCHI", "DEHRADUN", "GUWAHATI"
    ]

    for city in indian_cities:
        if city in address:
            return city

    if len(parts) >= 2:
        return normalize(parts[-2])

    return ""


def make_key(name, address, country):
    return (
        core_name(name),
        city_from_address(address, country),
        normalize(country),
    )


# ============================================================
# LOAD MASTER
# ============================================================

print("\n" + "=" * 70)
print("LOADING MASTER")
print("=" * 70)

master_index = {}

with open(
    MASTER_FILE,
    encoding="utf-8",
    errors="replace",
    newline=""
) as f:

    reader = csv.DictReader(
    f,
    fieldnames=[
        "entity_id",
        "business_name",
        "business_address",
        "country"
    ],
    delimiter="\t"
)

    for row in reader:

        entity_id = row["entity_id"]

        key = make_key(
            row["business_name"],
            row["business_address"],
            row["country"]
        )

        master_index.setdefault(key, set()).add(entity_id)

print(f"Master keys indexed: {len(master_index):,}")


# ============================================================
# LOAD GOLDEN TRUTH
# ============================================================

print("\n" + "=" * 70)
print("LOADING GOLDEN TRUTH")
print("=" * 70)

gold = {}

with open(
    GROUND_TRUTH,
    encoding="utf-8",
    errors="replace",
    newline=""
) as f:

    reader = csv.DictReader(f, delimiter="\t")

    for row in reader:

        master_id = row["source1_entity_id"]

        for source_id in row["matched_entity_ids"].split(","):

            source_id = source_id.strip()

            if source_id:
                gold.setdefault(source_id, set()).add(master_id)

print(f"Golden-truth source IDs: {len(gold):,}")


# ============================================================
# EVALUATION
# ============================================================

def evaluate(source_name, filename):

    print("\n" + "=" * 70)
    print(f"EVALUATING {source_name}")
    print("=" * 70)

    total = 0
    records_with_truth = 0

    hit = 0
    exact = 0

    total_predicted = 0
    total_correct = 0
    total_actual = 0

    with open(
        filename,
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as f:

        reader = csv.DictReader(f, delimiter="\t")

        for row in reader:

            total += 1

            source_id = row["entity_id"]

            key = make_key(
                row["business_name"],
                row["business_address"],
                row["country"]
            )

            predicted = master_index.get(key, set())

            actual = gold.get(source_id, set())

            if not actual:
                continue

            records_with_truth += 1

            correct = predicted & actual

            if correct:
                hit += 1

            if predicted == actual:
                exact += 1

            total_predicted += len(predicted)
            total_correct += len(correct)
            total_actual += len(actual)

            if total % 100000 == 0:
                print(
                    f"{source_name}: "
                    f"{total:,} processed | "
                    f"{hit:,} hit"
                )

    if records_with_truth == 0:
        print("NO GOLDEN TRUTH FOUND")
        return

    hit_accuracy = hit / records_with_truth * 100

    exact_accuracy = exact / records_with_truth * 100

    precision = (
        total_correct / total_predicted * 100
        if total_predicted else 0
    )

    recall = (
        total_correct / total_actual * 100
        if total_actual else 0
    )

    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0
    )

    print("\n" + "-" * 70)

    print(f"Total records       : {total:,}")
    print(f"Records with truth  : {records_with_truth:,}")
    print(f"Predicted matches   : {total_predicted:,}")
    print(f"Correct predictions : {total_correct:,}")
    print()

    print(f"Hit accuracy        : {hit_accuracy:.2f}%")
    print(f"Exact-set accuracy  : {exact_accuracy:.2f}%")
    print(f"Precision           : {precision:.2f}%")
    print(f"Recall              : {recall:.2f}%")
    print(f"F1 score            : {f1:.2f}%")

    print("-" * 70)


# ============================================================
# RUN
# ============================================================

evaluate("SOURCE2", SOURCE2_FILE)
evaluate("SOURCE3", SOURCE3_FILE)

print("\n" + "=" * 70)
print("BASELINE EVALUATION COMPLETE")
print("=" * 70)

print("\nRule tested:")
print("CORE NAME + CITY + COUNTRY")
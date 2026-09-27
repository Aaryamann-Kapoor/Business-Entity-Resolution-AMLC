import csv

GROUND_TRUTH = "train_ground_truth.tsv"

SOURCE_FILES = {
    "SOURCE2": "data/address_scored_source2.tsv",
    "SOURCE3": "data/address_scored_source3.tsv",
}


# ============================================================
# LOAD GOLDEN TRUTH
# ============================================================

print("=" * 70)
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
# EVALUATE
# ============================================================

def evaluate(source_name, filename):

    print()
    print("=" * 70)
    print(f"EVALUATING {source_name}")
    print("=" * 70)

    total = 0
    records_with_truth = 0

    # We evaluate different thresholds.
    thresholds = [
        0.50,
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
        0.90,
        0.95,
    ]

    results = {}

    for threshold in thresholds:

        results[threshold] = {
            "predicted": 0,
            "correct": 0,
            "actual": 0,
            "hit": 0,
            "exact": 0,
        }

    # --------------------------------------------------------
    # READ CANDIDATES
    # --------------------------------------------------------

    with open(
        filename,
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as f:

        reader = csv.DictReader(
            f,
            delimiter="\t"
        )

        for row in reader:

            total += 1

            source_id = row["source_entity_id"]

            actual = gold.get(source_id, set())

            if not actual:
                continue

            records_with_truth += 1

            candidate_id = row["candidate_entity_id"]

            score = float(
                row["address_score"]
            )

            for threshold in thresholds:

                if score >= threshold:

                    results[threshold]["predicted"] += 1

                    results[threshold]["actual"] += len(actual)

                    if candidate_id in actual:

                        results[threshold]["correct"] += 1

                        results[threshold]["hit"] += 1

            if total % 500000 == 0:

                print(
                    f"Processed: {total:,}"
                )

    # --------------------------------------------------------
    # PRINT RESULTS
    # --------------------------------------------------------

    print()
    print(f"Total candidates       : {total:,}")
    print(f"Records with truth     : {records_with_truth:,}")

    print()
    print(
        f"{'THRESHOLD':<12}"
        f"{'PRECISION':<15}"
        f"{'RECALL':<15}"
        f"{'F1':<15}"
        f"{'CORRECT':<15}"
    )

    print("-" * 72)

    best_threshold = None
    best_f1 = -1

    for threshold in thresholds:

        predicted = results[threshold]["predicted"]
        correct = results[threshold]["correct"]
        actual = results[threshold]["actual"]

        precision = (
            correct / predicted
            if predicted
            else 0
        )

        recall = (
            correct / actual
            if actual
            else 0
        )

        f1 = (
            2 * precision * recall / (precision + recall)
            if precision + recall
            else 0
        )

        print(
            f"{threshold:<12.2f}"
            f"{precision * 100:<15.2f}"
            f"{recall * 100:<15.2f}"
            f"{f1 * 100:<15.2f}"
            f"{correct:<15,}"
        )

        if f1 > best_f1:

            best_f1 = f1
            best_threshold = threshold

    print()
    print("-" * 72)

    print(
        f"Best threshold by F1: "
        f"{best_threshold:.2f}"
    )

    print(
        f"Best F1 score: "
        f"{best_f1 * 100:.2f}%"
    )

    print("-" * 72)


# ============================================================
# RUN
# ============================================================

evaluate(
    "SOURCE2",
    SOURCE_FILES["SOURCE2"]
)

evaluate(
    "SOURCE3",
    SOURCE_FILES["SOURCE3"]
)

print()
print("=" * 70)
print("ADDRESS EVALUATION COMPLETE")
print("=" * 70)
import csv
from collections import defaultdict

TRUTH_FILE = "train_ground_truth.tsv"
SOURCE2_FILE = "data/address_scored_source2.tsv"
SOURCE3_FILE = "data/address_scored_source3.tsv"


print("=" * 70)
print("LOADING GOLDEN TRUTH")
print("=" * 70)

# source_entity_id -> set of correct S1 IDs
truth = defaultdict(set)

with open(TRUTH_FILE, "r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f, delimiter="\t")

    for row in reader:
        s1 = row["source1_entity_id"].strip()

        for entity_id in row["matched_entity_ids"].split(","):
            entity_id = entity_id.strip()
            if entity_id:
                truth[entity_id].add(s1)

print("Golden source records:", len(truth))


def evaluate(filename, source_name):

    print("\n" + "=" * 70)
    print(f"EVALUATING {source_name}")
    print("=" * 70)

    # source entity -> best candidate
    best = {}

    with open(filename, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")

        for row in reader:

            source_id = row["source_entity_id"].strip()
            candidate_id = row["candidate_entity_id"].strip()
            decision = row["decision"].strip()
            score = float(row["address_score"])

            if decision != "MATCH":
                continue

            if (
                source_id not in best
                or score > best[source_id][1]
            ):
                best[source_id] = (candidate_id, score)

    total_predicted = len(best)

    correct = 0
    wrong = 0
    no_truth = 0

    for source_id, (candidate_id, score) in best.items():

        if source_id not in truth:
            no_truth += 1
            continue

        if candidate_id in truth[source_id]:
            correct += 1
        else:
            wrong += 1

    evaluated = correct + wrong

    precision = (
        correct / total_predicted * 100
        if total_predicted else 0
    )

    recall = (
        correct / len(truth) * 100
        if truth else 0
    )

    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0
    )

    print()
    print(f"Predicted MATCH records : {total_predicted:,}")
    print(f"Correct                 : {correct:,}")
    print(f"Wrong                   : {wrong:,}")
    print(f"No truth                : {no_truth:,}")
    print("-" * 70)
    print(f"Precision               : {precision:.2f}%")
    print(f"Recall                  : {recall:.2f}%")
    print(f"F1                      : {f1:.2f}%")
    print("-" * 70)


evaluate(SOURCE2_FILE, "SOURCE 2")
evaluate(SOURCE3_FILE, "SOURCE 3")

print("\n" + "=" * 70)
print("ADDRESS vs GOLDEN TRUTH EVALUATION COMPLETE")
print("=" * 70)

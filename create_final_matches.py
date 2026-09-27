import csv
import os

THRESHOLD = 0.95

INPUT_FILES = {
    "SOURCE2": "data/address_scored_source2.tsv",
    "SOURCE3": "data/address_scored_source3.tsv",
}

OUTPUT_DIR = "data/final"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def process_source(source_name, input_file):

    matched_file = os.path.join(
        OUTPUT_DIR,
        f"{source_name.lower()}_matched.tsv"
    )

    unmatched_file = os.path.join(
        OUTPUT_DIR,
        f"{source_name.lower()}_unmatched.tsv"
    )

    matched = 0
    unmatched = 0
    total = 0

    print()
    print("=" * 70)
    print(f"PROCESSING {source_name}")
    print("=" * 70)

    with open(
        input_file,
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as infile:

        reader = csv.DictReader(
            infile,
            delimiter="\t"
        )

        fieldnames = reader.fieldnames

        if "address_score" not in fieldnames:
            raise ValueError(
                f"address_score column not found in {input_file}"
            )

        matched_fields = fieldnames + [
            "final_status",
            "final_threshold"
        ]

        unmatched_fields = fieldnames + [
            "final_status",
            "final_threshold"
        ]

        with open(
            matched_file,
            "w",
            encoding="utf-8",
            newline=""
        ) as mf, open(
            unmatched_file,
            "w",
            encoding="utf-8",
            newline=""
        ) as uf:

            matched_writer = csv.DictWriter(
                mf,
                fieldnames=matched_fields,
                delimiter="\t"
            )

            unmatched_writer = csv.DictWriter(
                uf,
                fieldnames=unmatched_fields,
                delimiter="\t"
            )

            matched_writer.writeheader()
            unmatched_writer.writeheader()

            for row in reader:

                total += 1

                try:
                    score = float(row["address_score"])
                except (ValueError, TypeError):
                    score = 0.0

                output_row = dict(row)

                output_row["final_threshold"] = THRESHOLD

                if score >= THRESHOLD:

                    output_row["final_status"] = "MATCHED"

                    matched_writer.writerow(output_row)

                    matched += 1

                else:

                    output_row["final_status"] = "UNMATCHED"

                    unmatched_writer.writerow(output_row)

                    unmatched += 1

                if total % 500000 == 0:

                    print(
                        f"Processed: {total:,} | "
                        f"Matched: {matched:,} | "
                        f"Unmatched: {unmatched:,}"
                    )

    print()
    print(f"Total      : {total:,}")
    print(f"Matched    : {matched:,}")
    print(f"Unmatched  : {unmatched:,}")

    if total:
        print(
            f"Match rate : "
            f"{matched / total * 100:.2f}%"
        )

    print()
    print(f"Matched file  : {matched_file}")
    print(f"Unmatched file: {unmatched_file}")


# ============================================================
# RUN
# ============================================================

process_source(
    "SOURCE2",
    INPUT_FILES["SOURCE2"]
)

process_source(
    "SOURCE3",
    INPUT_FILES["SOURCE3"]
)

print()
print("=" * 70)
print("FINAL MATCH / UNMATCHED GENERATION COMPLETE")
print("=" * 70)

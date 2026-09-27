# Business Entity Resolution — Team Bodhi

## Overview

This solution performs **Business Entity Resolution** by matching records from two source datasets (SOURCE2, SOURCE3) against a reference/master dataset (SOURCE1). The pipeline uses PySpark for scalable data processing with a blocking + scoring approach to efficiently resolve entities across millions of records.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   SOURCE 1   │     │   SOURCE 2   │     │   SOURCE 3   │
│  (Master /   │     │  (Resolve)   │     │  (Resolve)   │
│  Reference)  │     │              │     │              │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────┬───────┴────────────────────┘
                    │
            ┌───────▼───────┐
            │  Normalization │  ← uppercase, strip punctuation,
            │  & Cleaning    │    remove legal suffixes
            └───────┬───────┘
                    │
            ┌───────▼───────┐
            │   Blocking     │  ← core_name | city | country
            │                │    reduces comparisons
            └───────┬───────┘
                    │
            ┌───────▼───────┐
            │   Scoring      │  ← Levenshtein similarity,
            │   & Ranking    │    weighted feature scoring
            └───────┬───────┘
                    │
            ┌───────▼───────┐
            │   Best Match   │  ← top-1 per source entity
            │   Selection    │
            └───────┬───────┘
                    │
        ┌───────────▼───────────┐
        │  matching_results.tsv  │
        │  candidate_pairs.tsv   │
        └────────────────────────┘
```

## Input Files

The program expects three TSV files provided by the evaluation environment:

| Argument    | Description                              |
|-------------|------------------------------------------|
| `--source1` | Reference / master entity dataset        |
| `--source2` | First source dataset to be resolved      |
| `--source3` | Second source dataset to be resolved     |

Each dataset must contain the following columns:

| Column             | Description                        |
|--------------------|------------------------------------|
| `entity_id`        | Unique identifier for the entity   |
| `business_name`    | Name of the business               |
| `business_address` | Full business address              |
| `country`          | Country of the business            |

## Pipeline Steps

### 1. Data Normalization

- **Whitespace** — collapse multiple spaces, trim leading/trailing
- **Punctuation** — strip all non-alphanumeric characters
- **Case** — convert to uppercase for consistent comparison
- **Legal suffixes** — remove common suffixes:
  `PRIVATE LIMITED`, `PVT LTD`, `PVT LIMITED`, `LIMITED`, `LTD`, `INCORPORATED`, `INC`, `CORPORATION`, `CORP`, `COMPANY`, `CO`, `LLC`

### 2. City Extraction

Extracts the city from the `business_address` field by taking the last comma-separated component (conservative heuristic).

### 3. Blocking (Candidate Generation)

Generates a composite **block key** from:

```
core_name | city | country
```

Only pairs that share the same block key are compared, dramatically reducing the $O(n^2)$ comparison space. Records with empty core_name, city, or country are excluded from blocking.

### 4. Similarity Scoring

For each candidate pair the system computes:

| Feature             | Weight | Method                   |
|---------------------|--------|--------------------------|
| `name_similarity`   | 0.70   | Levenshtein distance     |
| `core_match`        | 0.15   | Exact equality           |
| `city_match`        | 0.10   | Exact equality           |
| `country_match`     | 0.05   | Exact equality           |

**Name similarity** is calculated as:

$$
\text{similarity} = 1 - \frac{\text{levenshtein}(s, m)}{\max(\lvert s \rvert, \lvert m \rvert)}
$$

**Final score:**

$$
\text{score} = 0.70 \times \text{name\_similarity} + 0.15 \times \text{core\_match} + 0.10 \times \text{city\_match} + 0.05 \times \text{country\_match}
$$

### 5. Best Match Selection

For each source entity, candidates are ranked by `match_score` (descending), with `name_similarity` as a tiebreaker. The top-1 candidate is selected as the final match.

## How to Reproduce

### Prerequisites

- **Python** ≥ 3.9
- **Java** ≥ 8 (required by PySpark / Apache Spark)

### Setup

```bash
pip install -r requirements.txt
```

### Run

```bash
python src/run.py \
    --source1 /path/to/source1.tsv \
    --source2 /path/to/source2.tsv \
    --source3 /path/to/source3.tsv \
    --output  /path/to/output
```

### Output

The pipeline produces two files in the output directory:

| File                    | Description                                      |
|-------------------------|--------------------------------------------------|
| `matching_results.tsv`  | Final best matches (upload to leaderboard)       |
| `candidate_pairs.tsv`   | All blocking candidate pairs before scoring      |

### Output Columns (`matching_results.tsv`)

| Column                     | Description                            |
|----------------------------|----------------------------------------|
| `source`                   | Source label (`SOURCE2` / `SOURCE3`)   |
| `source_entity_id`         | Entity ID from the source dataset      |
| `source_business_name`     | Original business name from source     |
| `source_business_address`  | Original address from source           |
| `source_country`           | Country from source                    |
| `matched_entity_id`        | Best-matched entity ID from SOURCE1    |
| `matched_business_name`    | Business name from SOURCE1             |
| `matched_business_address` | Address from SOURCE1                   |
| `name_similarity`          | Levenshtein-based name similarity      |
| `match_score`              | Weighted composite match score         |

## Spark Configuration

| Parameter                            | Value     |
|--------------------------------------|-----------|
| `spark.driver.memory`                | `8g`      |
| `spark.sql.shuffle.partitions`       | `200`     |
| `spark.sql.autoBroadcastJoinThreshold` | `50m`   |

## Project Structure

```
bodhi_submission/
├── output/
│   ├── matching_results.tsv       # final matches (leaderboard upload)
│   └── candidate_pairs.tsv        # blocking candidate set
├── code/
│   └── business_entity_resolution/
│       ├── src/
│       │   └── run.py             # main pipeline script
│       ├── README.md              # this file
│       └── requirements.txt       # pinned dependencies
└── Documentation_template.md      # methodology write-up
```

## Team

**Team Bodhi**

# Business Entity Resolution — Team Bodhi

## Overview

This solution performs **Business Entity Resolution** by matching records from two source datasets (SOURCE2, SOURCE3) against a reference/master dataset (SOURCE1). The pipeline uses PySpark for scalable data processing with a **multi-pass blocking** + **multi-signal scoring** approach to efficiently resolve entities across millions of records.

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
            │  & Cleaning    │    remove legal suffixes, tokenize
            └───────┬───────┘
                    │
            ┌───────▼────────────────────────────┐
            │  Multi-Pass Blocking               │
            │                                    │
            │  Pass 1: core_name + country       │
            │  Pass 2: prefix(5) + city + country│
            │  Pass 3: core_name + city          │
            └───────┬────────────────────────────┘
                    │
            ┌───────▼───────┐
            │   Scoring      │  ← Levenshtein + Jaccard +
            │   (6 signals)  │    address + binary features
            └───────┬───────┘
                    │
            ┌───────▼───────┐
            │   Threshold    │  ← score ≥ 0.45
            │   & Ranking    │    top-1 per source entity
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
- **Legal suffixes** — remove common suffixes (English, German, French, Spanish, Dutch, Malaysian, Japanese, Finnish, Swedish, Turkish):
  `PRIVATE LIMITED`, `PVT LTD`, `LIMITED`, `LTD`, `INC`, `CORP`, `LLC`, `LLP`, `PLC`, `GMBH`, `AG`, `SA`, `SAS`, `NV`, `BV`, `PTY LTD`, `SDN BHD`, `KK`, `OY`, `AB`, `AS`, etc.
- **Tokenization** — split core name into word tokens for Jaccard similarity
- **Prefix extraction** — first 5 characters of core name for loose blocking

### 2. City Extraction

Extracts the city from `business_address` by taking the last comma-separated component. If that component looks like a zip/postal code (all digits), falls back to the second-to-last component.

### 3. Multi-Pass Blocking (Candidate Generation)

Three blocking passes with different key strategies, unioned and deduplicated:

| Pass | Block Key                          | Catches                                     |
|------|------------------------------------|----------------------------------------------|
| 1    | `core_name` + `country`            | City typos, missing city data                |
| 2    | `name_prefix(5)` + `city` + `country` | Core name typos sharing the same prefix  |
| 3    | `core_name` + `city`              | Country mismatches, multinational entities    |

### 4. Similarity Scoring (6 Signals)

| Feature              | Weight | Method                                      |
|----------------------|--------|----------------------------------------------|
| `name_similarity`    | 0.35   | Levenshtein distance (character-level)       |
| `jaccard_similarity` | 0.25   | Jaccard index on word tokens                 |
| `core_match`         | 0.15   | Exact equality of core names                 |
| `address_similarity` | 0.10   | Levenshtein distance on cleaned addresses    |
| `city_match`         | 0.10   | Exact equality of cities                     |
| `country_match`      | 0.05   | Exact equality of countries                  |

**Final score:**

$$\text{score} = 0.35 \times \text{name\_sim} + 0.25 \times \text{jaccard} + 0.15 \times \text{core} + 0.10 \times \text{addr\_sim} + 0.10 \times \text{city} + 0.05 \times \text{country}$$

### 5. Best Match Selection

- Candidates ranked by `match_score` → `name_similarity` → `jaccard_similarity` (descending)
- Top-1 candidate per source entity
- Minimum threshold of **0.45** applied to prevent false positives

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

| File                    | Description                                      |
|-------------------------|--------------------------------------------------|
| `matching_results.tsv`  | Final best matches (upload to leaderboard)       |
| `candidate_pairs.tsv`   | All blocking candidate pairs before scoring      |

Both are written as proper single-file TSVs (not Spark part-file directories).

## Spark Configuration

| Parameter                              | Value   |
|----------------------------------------|---------|
| `spark.driver.memory`                  | `8g`    |
| `spark.sql.shuffle.partitions`         | `200`   |
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

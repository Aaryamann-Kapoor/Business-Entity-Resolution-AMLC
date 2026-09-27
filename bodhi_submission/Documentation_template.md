# Documentation — Team Bodhi

## 1. Problem Statement

Business entity resolution is the task of determining whether records from different data sources refer to the same real-world business entity. Given a master/reference dataset (SOURCE1) and two additional source datasets (SOURCE2, SOURCE3), the goal is to find the best matching reference entity for each record in the source datasets.

This is a challenging problem because business names and addresses are often inconsistently formatted, abbreviated, or contain noise across different data sources.

## 2. Data Description

### Input

The evaluation environment provides three TSV files:

| Dataset  | Role                                              |
|----------|---------------------------------------------------|
| SOURCE1  | Reference / master business entities              |
| SOURCE2  | First set of business records to resolve          |
| SOURCE3  | Second set of business records to resolve         |

Each dataset contains the following key fields:

- `entity_id` — unique identifier
- `business_name` — business name string
- `business_address` — full address (comma-separated components)
- `country` — country of incorporation / registration

**Note:** The submission does not contain the datasets. They are provided at runtime by the evaluation environment.

## 3. Methodology

### 3.1 Data Preprocessing & Normalization

The pipeline applies the following normalization steps to all three datasets:

1. **Whitespace normalization** — collapse multiple whitespace characters into a single space and trim leading/trailing whitespace.
2. **Punctuation removal** — strip all non-alphanumeric characters, replacing them with spaces.
3. **Case normalization** — convert all text to uppercase for case-insensitive comparison.
4. **Legal suffix removal** — strip common legal entity suffixes from the end of business names:
   - `PRIVATE LIMITED`, `PVT LTD`, `PVT LIMITED`
   - `LIMITED`, `LTD`
   - `INCORPORATED`, `INC`
   - `CORPORATION`, `CORP`
   - `COMPANY`, `CO`
   - `LLC`
5. **City extraction** — extract the city from the `business_address` by taking the last comma-separated component (a conservative heuristic that works across diverse address formats).
6. **Country normalization** — uppercase and trim the country field.

### 3.2 Blocking (Candidate Generation)

To avoid an infeasible all-pairs comparison, the pipeline employs a **blocking strategy** that generates a composite key:

```
block_key = core_name | city | country
```

Only record pairs that share the same block key are considered as potential matches. This reduces the comparison space from O(N × M) to O(B × k), where B is the number of blocks and k is the average block size.

**Filtering:** Records with empty `core_name`, `city`, or `country` are excluded from blocking to avoid spurious matches.

### 3.3 Similarity Computation

For each candidate pair (source record, master record), the pipeline computes four features:

#### Levenshtein-based Name Similarity

The name similarity is computed using the Levenshtein (edit) distance on cleaned, lowercase business names:

$$
\text{name\_similarity} = 1 - \frac{\text{levenshtein}(s, m)}{\max(|s|, |m|)}
$$

This yields a value in [0, 1], where 1 indicates identical names and 0 indicates completely dissimilar names.

#### Binary Feature Signals

| Feature          | Value | Condition                                    |
|------------------|-------|----------------------------------------------|
| `core_match`     | 1 / 0 | Source and master `core_name` are identical  |
| `city_match`     | 1 / 0 | Source and master `city` are identical       |
| `country_match`  | 1 / 0 | Source and master `country` are identical    |

### 3.4 Scoring

The four features are combined into a weighted composite score:

$$
\text{match\_score} = 0.70 \times \text{name\_similarity} + 0.15 \times \text{core\_match} + 0.10 \times \text{city\_match} + 0.05 \times \text{country\_match}
$$

**Weight rationale:**
- **Name similarity (70%)** — the strongest discriminative signal; even within the same block, names may differ in abbreviation, transliteration, or minor errors.
- **Core name match (15%)** — rewards exact alignment of the cleaned core business name (without legal suffixes).
- **City match (10%)** — geographical agreement provides additional confirmation.
- **Country match (5%)** — least discriminative within a block (since blocking already enforces country agreement), but included for completeness.

### 3.5 Best Match Selection

For each source entity, all candidates are ranked by:
1. `match_score` (descending)
2. `name_similarity` (descending, tiebreaker)

The top-ranked candidate is selected as the final match using a PySpark window function with `row_number()`.

## 4. Technical Stack

| Component       | Technology                  |
|-----------------|-----------------------------|
| Language        | Python 3.9+                 |
| Processing      | Apache Spark (PySpark)      |
| Similarity      | Levenshtein distance        |
| Normalization   | Regex, string operations    |

### Spark Configuration

| Parameter                              | Value   | Reason                                    |
|----------------------------------------|---------|-------------------------------------------|
| `spark.driver.memory`                  | `8g`    | Handle large in-memory joins              |
| `spark.sql.shuffle.partitions`         | `200`   | Balance parallelism and overhead          |
| `spark.sql.autoBroadcastJoinThreshold` | `50m`   | Auto-broadcast smaller tables in joins    |

## 5. Outputs

The pipeline produces two output files:

| File                    | Description                                                     |
|-------------------------|-----------------------------------------------------------------|
| `matching_results.tsv`  | Best match for each source entity (leaderboard submission file) |
| `candidate_pairs.tsv`   | All candidate pairs generated during blocking                   |

### Output Schema (`matching_results.tsv`)

| Column                     | Type    | Description                          |
|----------------------------|---------|--------------------------------------|
| `source`                   | string  | Source label (`SOURCE2` / `SOURCE3`) |
| `source_entity_id`         | string  | Entity ID from source dataset        |
| `source_business_name`     | string  | Original business name               |
| `source_business_address`  | string  | Original business address            |
| `source_country`           | string  | Country from source                  |
| `matched_entity_id`        | string  | Best-matched entity ID from SOURCE1  |
| `matched_business_name`    | string  | Business name from SOURCE1           |
| `matched_business_address` | string  | Address from SOURCE1                 |
| `name_similarity`          | float   | Levenshtein-based similarity [0, 1]  |
| `match_score`              | float   | Weighted composite score [0, 1]      |

## 6. Reproduction Steps

### Prerequisites

- Python ≥ 3.9
- Java ≥ 8 (required by Apache Spark)

### Installation

```bash
cd code/business_entity_resolution
pip install -r requirements.txt
```

### Execution

```bash
python src/run.py \
    --source1 /path/to/source1.tsv \
    --source2 /path/to/source2.tsv \
    --source3 /path/to/source3.tsv \
    --output  ../../output
```

The `--output` flag specifies the directory where `matching_results.tsv` will be written. The directory is created automatically if it does not exist.

## 7. Assumptions & Limitations

- **Address parsing** — city extraction uses a simple last-component heuristic, which may not generalize to all address formats.
- **Blocking completeness** — the composite blocking key requires exact agreement on `core_name`, `city`, and `country`. True matches with typos in any of these three components will be missed.
- **No ML model** — the scoring function uses fixed hand-tuned weights rather than a learned model. This keeps the solution simple and interpretable but may not capture complex matching patterns.
- **Single-source resolution** — each source entity is matched independently; cross-source transitivity is not exploited.

## 8. Team

**Team Bodhi**

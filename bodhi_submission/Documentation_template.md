# Documentation — Team Bodhi

## 1. Problem Statement

Business entity resolution is the task of determining whether records from different data sources refer to the same real-world business entity. Given a master/reference dataset (SOURCE1) and two additional source datasets (SOURCE2, SOURCE3), the goal is to find the best matching reference entity for each record in the source datasets.

This is a challenging problem because business names and addresses are often inconsistently formatted, abbreviated, transliterated, or contain noise across different data sources.

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

1. **Whitespace normalization** — collapse multiple whitespace characters into a single space and trim.
2. **Punctuation removal** — strip all non-alphanumeric characters, replacing them with spaces.
3. **Case normalization** — convert all text to uppercase for case-insensitive comparison.
4. **Legal suffix removal** — strip common legal entity suffixes from the end of business names. The list covers 30+ suffixes across multiple jurisdictions:
   - *English:* `PRIVATE LIMITED`, `PVT LTD`, `LIMITED`, `LTD`, `INC`, `CORP`, `LLC`, `LLP`, `PLC`
   - *German:* `GMBH`, `AG`
   - *French/Spanish/Italian:* `SA`, `SAS`, `SARL`, `SRL`, `SL`
   - *Dutch/Belgian:* `NV`, `BV`
   - *Other:* `PTY LTD`, `SDN BHD`, `KK`, `OY`, `AB`, `AS`
5. **City extraction** — extract the city from `business_address` by taking the last comma-separated component. If that component is purely numeric (a postal code), the second-to-last component is used instead.
6. **Country normalization** — uppercase and trim the country field.
7. **Prefix extraction** — the first 5 characters of the core name, used for loose blocking.
8. **Tokenization** — the core name is split into individual word tokens for Jaccard similarity.
9. **Address cleaning** — the full address is lowercased and stripped of non-alphanumeric characters for address-level similarity.

### 3.2 Multi-Pass Blocking (Candidate Generation)

To avoid an infeasible all-pairs comparison while maximizing recall, the pipeline uses **three complementary blocking passes**:

| Pass | Key                                    | Rationale                                             |
|------|----------------------------------------|-------------------------------------------------------|
| 1    | `core_name` + `country`               | Catches matches where city is missing, misspelled, or formatted differently |
| 2    | `name_prefix(5)` + `city` + `country` | Catches matches where the core name has small typos but shares the same prefix |
| 3    | `core_name` + `city`                  | Catches multinational entities or records with country mismatches |

The candidate pairs from all three passes are **unioned** and **deduplicated** on `(source_entity_id, master_entity_id)`. This multi-pass approach significantly improves recall over a single blocking key while keeping the candidate set manageable.

**Filtering:** Records with empty key components are excluded from each respective blocking pass to prevent spurious matches on empty strings.

### 3.3 Similarity Computation

For each candidate pair, the pipeline computes **six features** across two categories:

#### Continuous Similarity Features

**Levenshtein Name Similarity** — character-level edit distance on cleaned business names:

$$\text{name\_similarity} = 1 - \frac{\text{levenshtein}(s, m)}{\max(|s|, |m|)}$$

**Jaccard Token Similarity** — word-level overlap, robust to word reordering:

$$\text{jaccard\_similarity} = \frac{|\text{tokens}(s) \cap \text{tokens}(m)|}{|\text{tokens}(s) \cup \text{tokens}(m)|}$$

This is critical for cases like `"Steel Authority India"` vs `"India Steel Authority"` where Levenshtein alone would give a misleadingly low score.

**Address Similarity** — character-level edit distance on cleaned addresses:

$$\text{address\_similarity} = 1 - \frac{\text{levenshtein}(a_s, a_m)}{\max(|a_s|, |a_m|)}$$

Set to 0.0 if either address is empty or trivially short (≤ 1 character).

#### Binary Feature Signals

| Feature          | Value | Condition                                    |
|------------------|-------|----------------------------------------------|
| `core_match`     | 1 / 0 | Source and master `core_name` are identical  |
| `city_match`     | 1 / 0 | Source and master `city` are identical       |
| `country_match`  | 1 / 0 | Source and master `country` are identical    |

### 3.4 Scoring

The six features are combined into a weighted composite score:

$$\text{score} = 0.35 \times \text{name\_sim} + 0.25 \times \text{jaccard} + 0.15 \times \text{core\_match} + 0.10 \times \text{addr\_sim} + 0.10 \times \text{city\_match} + 0.05 \times \text{country\_match}$$

**Weight rationale:**

- **Name similarity (35%)** — strong discriminative signal at the character level; captures abbreviations and minor typos.
- **Jaccard similarity (25%)** — complements Levenshtein by handling word reordering, insertions of extra words (e.g., "India" prefix/suffix), and token-level abbreviation differences.
- **Core name match (15%)** — rewards exact alignment of the cleaned core business name. With multi-pass blocking, this is no longer guaranteed and becomes a meaningful signal.
- **Address similarity (10%)** — provides independent confirmation beyond just the name; differentiates same-named businesses at different locations.
- **City match (10%)** — geographical agreement, useful when blocking was done on core_name only (Pass 1, Pass 3).
- **Country match (5%)** — least discriminative but still valuable for Pass 3 (core_name + city blocking without country).

### 3.5 Threshold & Best Match Selection

1. For each source entity, all candidates are ranked by `match_score` (descending), with `name_similarity` and `jaccard_similarity` as tiebreakers.
2. The top-ranked candidate is selected using a PySpark window function with `row_number()`.
3. A **minimum score threshold of 0.45** is applied — matches below this are discarded to prevent false positives.

## 4. Technical Stack

| Component       | Technology                  |
|-----------------|-----------------------------|
| Language        | Python 3.9+                 |
| Processing      | Apache Spark (PySpark 3.5.3)|
| Similarity      | Levenshtein + Jaccard       |
| Normalization   | Regex, string operations    |
| Output          | Single-file TSV             |

### Spark Configuration

| Parameter                              | Value   | Reason                                    |
|----------------------------------------|---------|-------------------------------------------|
| `spark.driver.memory`                  | `8g`    | Handle large in-memory joins              |
| `spark.sql.shuffle.partitions`         | `200`   | Balance parallelism and overhead          |
| `spark.sql.autoBroadcastJoinThreshold` | `50m`   | Auto-broadcast smaller tables in joins    |

## 5. Outputs

The pipeline produces two output files as **single TSV files** (not Spark part-file directories):

| File                    | Description                                                     |
|-------------------------|-----------------------------------------------------------------|
| `matching_results.tsv`  | Best match for each source entity (leaderboard submission file) |
| `candidate_pairs.tsv`   | All candidate pairs generated during multi-pass blocking        |

### Output Schema — `matching_results.tsv`

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
| `jaccard_similarity`       | float   | Token-level Jaccard similarity [0, 1]|
| `address_similarity`       | float   | Address-level similarity [0, 1]      |
| `match_score`              | float   | Weighted composite score [0, 1]      |

### Output Schema — `candidate_pairs.tsv`

| Column                     | Type    | Description                          |
|----------------------------|---------|--------------------------------------|
| `source`                   | string  | Source label                         |
| `source_entity_id`         | string  | Entity ID from source                |
| `source_business_name`     | string  | Business name from source            |
| `source_core_name`         | string  | Normalized core name from source     |
| `source_city`              | string  | Extracted city from source           |
| `source_country`           | string  | Country from source                  |
| `master_entity_id`         | string  | Entity ID from master                |
| `master_business_name`     | string  | Business name from master            |
| `master_core_name`         | string  | Normalized core name from master     |
| `master_city`              | string  | Extracted city from master           |
| `master_country`           | string  | Country from master                  |
| `block_type`               | string  | Which blocking pass generated this pair |

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

The `--output` flag specifies the directory where output files will be written. The directory is created automatically if it does not exist.

## 7. Design Decisions

| Decision | Alternative considered | Why we chose this |
|----------|----------------------|-------------------|
| Multi-pass blocking | Single composite key | Single key was too strict, dropping true matches with any field variation |
| Jaccard + Levenshtein | Levenshtein only | Levenshtein penalizes word reordering; Jaccard handles it naturally |
| Fixed weights | Learned ML model | Simplicity, interpretability, and no training data dependency |
| Score threshold 0.45 | No threshold | Prevents low-quality matches from inflating false positive rate |
| Single-file TSV output | Spark part-file directory | Leaderboard expects a single uploadable file |

## 8. Assumptions & Limitations

- **Blocking completeness** — while multi-pass blocking significantly improves recall over a single key, entities with typos in both the core name *and* prefix will still be missed.
- **No ML model** — the scoring function uses fixed hand-tuned weights. A learned model could better capture non-linear feature interactions.
- **Single-source resolution** — each source entity is matched independently; cross-source transitivity is not exploited.
- **No phonetic matching** — names with phonetic variations (e.g., "Volkswagen" vs "Volkswagon") are not explicitly handled, though Levenshtein partially covers this.
- **Address parsing** — city extraction uses a heuristic (last comma-component, fallback for zip codes) that may not generalize to all address formats globally.

## 9. Team

**Team Bodhi**

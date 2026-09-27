# Business Entity Resolution

## 1. Problem

The system performs business entity resolution by matching records from SOURCE2 and SOURCE3 against the reference entities in SOURCE1.

## 2. Input

The evaluation environment provides three TSV files:

- SOURCE1: reference/master business entities
- SOURCE2: records to resolve
- SOURCE3: records to resolve

The submission does not contain the datasets.

## 3. Data Processing

The pipeline:

1. Reads the supplied TSV files.
2. Normalizes business names.
3. Removes common legal suffixes.
4. Normalizes business addresses.
5. Extracts city information.
6. Normalizes country information.

## 4. Candidate Generation

Candidate records are generated using blocking based on normalized country and city information.

This reduces the number of reference entities that need to be compared with each source record.

## 5. Matching

For each candidate pair, the system calculates:

- Business-name similarity
- Address similarity
- Core-name equality
- City agreement
- Country agreement

These signals are combined into a final matching score.

The candidate with the highest score for each source entity is selected as its best match.

## 6. Outputs

The pipeline produces:

- `matching_results.tsv` — selected best matches
- `candidate_pairs.tsv` — candidate pairs generated during blocking

## 7. Reproduction

Install the dependencies:

```bash
pip install -r requirements.txt

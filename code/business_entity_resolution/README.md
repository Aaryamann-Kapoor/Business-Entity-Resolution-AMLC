cat > submission/code/business_entity_resolution/README.md <<'EOF'
# Business Entity Resolution

## Overview

This solution performs Business Entity Resolution by matching records from two source datasets against a reference/master dataset.

The solution is designed to process the datasets supplied by the evaluation environment and does not require the training or testing datasets to be included in the submission.

## Input Files

The program accepts three TSV files:

- `source1` — reference/master entity dataset
- `source2` — first source dataset to be resolved
- `source3` — second source dataset to be resolved

Each dataset is expected to contain business entity information including fields such as:

- `entity_id`
- `business_name`
- `business_address`
- `country`

## Execution

Install the required dependency:

```bash
pip install -r requirements.txt

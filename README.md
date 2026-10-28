# Demo 4 — Tested and Reproducible Deployment

## Business problem

Can another engineer deploy the retail workflow without manually
recreating its pipeline and job?

This project packages our retail pipeline as version-controlled code,
tests its transformations, and deploys it to separate dev and test schemas.

## How it works

1. Auto Loader reads the original retail CSV from a Unity Catalog volume.
2. Bronze preserves the input rows and source metadata.
3. Silver contains accepted, typed transactions and calculated amounts.
4. Quarantine preserves rejected rows with a rejection reason.
5. Gold summarizes daily sales, cancellations, and net amounts.
6. A job runs the pipeline, then a verification notebook.
   Failed assertions cause the verification task to fail.

Code is developed locally and uploaded to Databricks using a bundle.
Databricks runs the pipeline and stores the output tables.

## Project structure

- `src/transformations.py`: reusable transformation functions.
- `src/retail_pipeline.py`: Bronze, Silver, quarantine, and Gold definitions.
- `tests/conftest.py`: local Spark test session.
- `tests/test_transformations.py`: unit tests with synthetic fixtures.
- `resources/retail_pipeline.yml`: pipeline configuration.
- `resources/retail_job.yml`: pipeline and verification task dependencies.
- `verify.ipynb`: parameterized integration checks.
- `databricks.yml`: bundle variables and deployment targets.
- `requirements-dev.txt`: pinned local testing dependencies.

## Dataset and rules

The source is the same unchanged UCI Online Retail CSV used in the
previous demos. It must already be available at:

`/Volumes/workspace/retail_demo/retail_source_files/landing`

The bundle does not upload the dataset. To use another location,
update `retail.landing_path` in `resources/retail_pipeline.yml`.

Main rules:

- Normalize invoice numbers while preserving original values.
- Parse invoice dates, quantities, and prices.
- Quarantine missing invoices, invalid values, non-positive prices,
  zero quantities, and inconsistent cancellation signs.
- Classify accepted invoices beginning with C as cancellations.
- Preserve negative cancellation amounts.
- Retain missing customer IDs when other checks pass.
- Preserve repeated rows; no deduplication is approved.
- Record the first matching rejection reason for each rejected row.

## Environments

| Target | Output schema |
|---|---|
| dev | workspace.retail_demo4_dev |
| test | workspace.retail_demo4_test |

Both targets read the same source CSV but have separate pipelines,
jobs, and output tables.

These are separate schemas in one workspace, not separate workspaces.

## Prerequisites

- A Databricks workspace with serverless compute available.
- Access to the source volume.
- Permission to create pipelines, jobs, schemas, and tables.
- Python 3.12 and Java 21 for the local test setup.
- Databricks CLI and Git.

Local versions used:
Python 3.12.10, Java 21.0.11, PySpark 4.0.1,
pytest 8.4.2, and Databricks CLI 1.22.1.

Databricks supplies the pipeline runtime separately from the local
PySpark installation.

## Local setup

Run these commands from the repository root in Windows Git Bash:

```bash
# Create and activate an isolated Python environment.
py -3.12 -m venv .venv
source .venv/Scripts/activate

# Install the pinned testing dependencies.
python -m pip install -r requirements-dev.txt

# Run the transformation tests.
python -m pytest tests -v --tb=short

# Authenticate to the intended Databricks workspace.
databricks auth login --profile retail-demo
```

## Deploy and run

Execute each command only after the previous command succeeds.

```bash
# Validate, deploy, and run the dev workflow.
databricks bundle validate -t dev --profile retail-demo
databricks bundle deploy -t dev --profile retail-demo
databricks bundle run retail_validation_job -t dev --profile retail-demo

# Deploy the same code and workflow to test.
databricks bundle validate -t test --profile retail-demo
databricks bundle deploy -t test --profile retail-demo
databricks bundle run retail_validation_job -t test --profile retail-demo
```

The job runs `update_pipeline`, followed by `verify_results`.
The verification notebook receives the full target schema as a parameter.

## Validation results

Four unit tests passed, covering:

- Transaction amounts and cancellation signs.
- Invoice number normalization.
- Date and numeric conversion.
- Rejection reasons and rule precedence.

A deliberate change from multiplication to addition caused an amount
test to fail. Restoring multiplication made the test pass.

The complete pipeline-and-verification job succeeded in both dev and test.

| Check | Expected result |
|---|---:|
| Bronze rows | 541,909 |
| Silver rows | 539,392 |
| Quarantine rows | 2,517 |
| Unaccounted rows | 0 |
| Gold daily summaries | 305 |
| Sales amount | 10,666,684.5440 |
| Cancellation amount | -896,812.4900 |
| Net amount | 9,769,872.0540 |

## Scope and limitations

- Verification uses fixed expected values for this unchanged dataset.
  Review those expectations before intentionally adding source files.
- The current notebook checks row counts and overall Gold totals.
  It does not independently compare every daily total with Silver.
- Local fixtures use Spark SQL VALUES to avoid a Windows Python-worker
  crash encountered with Python-list fixtures. The underlying worker
  issue was not resolved.
- Deployment is started manually through the CLI.
  Git tracks code changes; automated CI/CD is not configured.
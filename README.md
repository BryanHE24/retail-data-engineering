# Demo 4 — Tested and Reproducible Deployment

## Business problem
Can another engineer deploy the retail workflow without manually
recreating its pipeline and job?

## Goal
Reuse the business rules from the previous demos, test them,
and deploy the workflow to separate dev and test environments.

## Project structure
- src: reusable transformation code.
- tests: small synthetic fixtures and automated tests.
- resources: pipeline and job configuration.
- notebooks: workflow notebooks.

## Validation plan
1. Run unit tests for the business rules.
2. Introduce a deliberate defect and prove a test detects it.
3. Fix the defect and rerun the tests.
4. Validate the deployment bundle.
5. Deploy and verify separate dev and test targets.
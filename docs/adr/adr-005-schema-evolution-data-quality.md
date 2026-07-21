# ADR 05: Schema Evolution and Data Quality Policy

## Status
Approved

## Context
Retail data is notoriously dirty and prone to schema changes. Shopify APIs add/remove fields, POS registers send corrupted timestamps, and distributor CSV layouts change without notice. We must establish a policy on:
1. How we handle schema changes (Schema Evolution)
2. How we validate and enforce quality rules (Data Quality)

## Alternatives & Policy Choices

### 1. Schema Evolution
* **Bronze Layer Policy:** Schema-on-Read. The Bronze tables will store raw JSON/CSV structures. If columns are added or modified, we will leverage **Iceberg's Schema Evolution** capability. Since Iceberg maps columns by unique integer IDs (rather than names or physical offsets), columns can be added, renamed, or reordered without corrupting historical data.
* **Silver Layer Policy:** Schema Enforced. The Silver layer conformed entities must maintain a strict, typed schema. If a schema change occurs in Bronze, the PySpark transformation job will explicitly map the new/changed fields. If an incompatible schema change occurs (e.g., text instead of number), the Silver pipeline will fail.

### 2. Data Quality (DQ) Execution
* **Tool Selected:** **Great Expectations (GE)**.
* **Integration Point:** Run quality validation *after* writing to the Silver layer (or as a gatekeeper before finalizing the transaction).
* **Validation Levels:**
  - **Critical (Fail & Halt):** Check for null primary keys, duplicate records, invalid formats in critical fields (e.g. negative revenue, empty order IDs). These will trigger pipeline failures and prevent updating downstream Gold tables.
  - **Warning (Log & Proceed):** Check for missing optional attributes (e.g. missing customer emails, minor schema variations). These will log warnings but allow the pipeline to proceed.

## Decision
1. Standardize on **Iceberg's Native Schema Evolution** for handling structural changes safely.
2. Implement **Great Expectations** assertions as a mandatory verification step in the Silver processing job.

## Rationale
* Iceberg's ID-based tracking makes schema updates (like renaming or adding columns) instant and safe, avoiding the need for costly database migrations or full data rewrites.
* Separating validation into "Critical" and "Warning" alerts ensures our analytics are shielded from major data corruption while preventing minor, ignorable anomalies from constantly halting pipeline operations.

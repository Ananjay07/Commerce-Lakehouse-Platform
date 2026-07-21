# ADR 02: Catalog Selection

## Status
Approved

## Context
Apache Iceberg tables require a catalog to track table state and manage pointers to metadata files. The catalog guarantees atomic commits and coordinates updates across multiple readers and writers.

We need to select a catalog database or service for CartCo:
1. **Hive Metastore (HMS)**
2. **AWS Glue Data Catalog**
3. **Project Nessie**

## Alternatives Considered

### 1. Hive Metastore (HMS)
* **Pros:**
  - The industry veteran; highly compatible with legacy systems.
* **Cons:**
  - Requires maintaining a relational database (MySQL/Postgres) and the HMS service.
  - Slow metadata operations under high concurrency.
  - Lacks advanced versioning or Git-like semantics.

### 2. AWS Glue Data Catalog
* **Pros:**
  - Fully managed on AWS, integrates natively with Athena, EMR, and Redshift.
* **Cons:**
  - Vendor lock-in (hard to run locally for development and testing).
  - Can become expensive with millions of partition/table metadata requests.

### 3. Project Nessie
* **Pros:**
  - Git-like catalog versioning: supports branches, tags, and commits.
  - Allows zero-copy cloning and running pipeline transformations on a branch before merging to `main`.
  - Lightweight and easy to run locally in a Docker container.
  - First-class support for Apache Iceberg.
* **Cons:**
  - Relatively new compared to Hive Metastore, but widely adopted for Iceberg deployments.

## Decision
We will use **Project Nessie** as the catalog for CartCo's Lakehouse.

## Rationale
1. **Developer Experience:** We can run the exact same Nessie catalog locally in Docker as we would in staging or production.
2. **Git-like Branching:** In data engineering, pipelines frequently fail mid-way. With Nessie, we can run Spark jobs on a separate branch (e.g. `airflow-job-123`), verify the data, and merge it into `main` atomically. If the job fails, we simply discard the branch, leaving `main` completely clean and unaffected.
3. **Time Travel and Auditability:** Nessie maintains a history of commits, allowing us to query the state of the entire catalog at any commit hash or timestamp.

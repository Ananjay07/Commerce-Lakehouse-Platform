# ADR 01: Open Table Format Selection

## Status
Approved

## Context
CartCo needs to store multi-channel commerce data (Shopify, Amazon, in-store POS, distributor sales) in an S3/MinIO lakehouse. The raw data needs to be structured into tables that support ACID transactions, time-travel queries, schema evolution, and high performance for concurrent reads/writes.

We need to choose between the leading open table formats:
1. **Apache Iceberg**
2. **Delta Lake**
3. **Apache Hudi**

## Alternatives Considered

### 1. Apache Iceberg
* **Pros:**
  - True hidden partitioning (users don't have to specify partition columns in queries; Iceberg handles pruning automatically).
  - Clean separation of catalog and table metadata.
  - Safe, complete schema evolution (column renames/drops are tracked by ID, preventing data corruption or incorrect mapping).
  - Multi-engine support (Spark, Flink, Trino, Presto, DuckDB).
  - Strong Git-like branching support when paired with Project Nessie.
* **Cons:**
  - Slightly newer ecosystem than Delta Lake, but highly mature as of 2026.

### 2. Delta Lake
* **Pros:**
  - Extremely fast performance when tightly integrated with Databricks.
  - Large community and extensive production history.
* **Cons:**
  - Historically tied to Spark and Databricks (though open-source Delta has improved).
  - Partitioning requires explicit directory structures, leading to user query errors if partition keys are omitted.
  - Catalog versioning is less flexible compared to Project Nessie for Iceberg.

### 3. Apache Hudi
* **Pros:**
  - Designed for near-real-time streaming ingestion with fast upsert/delete capabilities (Copy-on-Write and Merge-on-Read).
* **Cons:**
  - High operational complexity and steep learning curve.
  - Primarily suited for low-latency streaming write use cases rather than general-purpose analytics.

## Decision
We will use **Apache Iceberg** as the open table format for CartCo's Medallion Lakehouse.

## Rationale
1. **Hidden Partitioning:** This avoids the "partition evolution" bottleneck where changing partition schemes requires rewriting all tables. Analysts don't need to know the physical layout to write fast queries.
2. **Catalog Independence & Nessie Integration:** The ability to run Git-like catalog branching via Nessie allows us to write and test data transformations on a feature branch before merging to main, ensuring raw data is never exposed mid-transaction.
3. **Robust Schema Evolution:** Schema changes (like dropping or renaming a column) are safe and do not corrupt historical data, which is highly common in retail systems where APIs evolve.

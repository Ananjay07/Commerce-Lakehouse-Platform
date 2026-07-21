# ADR 04: Ingestion Pattern

## Status
Approved

## Context
CartCo needs to ingest data from 3 primary sources for the initial platform foundation:
1. Shopify (Web sales - JSON from GraphQL API)
2. Physical POS stores (In-store sales - real-time/streaming Kafka events)
3. Distributor reports (Wholesale drops - daily CSV files)

We must choose our ingestion architecture and tooling:
1. **Airbyte / Fivetran** (Managed ELT)
2. **Kafka Connect** (Streaming Ingestion Framework)
3. **Custom PySpark & Python loaders** (Code-first approach)

## Alternatives Considered

### 1. Airbyte / Fivetran
* **Pros:**
  - Zero-code setup for standard sources (MySQL, Shopify, etc.).
* **Cons:**
  - High container footprint and memory overhead for local testing.
  - Less flexibility in handling real-time streaming topics (like our POS Kafka feed).
  - Lack of fine-grained control over Iceberg write patterns (e.g. partition keys, metadata commits).

### 2. Kafka Connect
* **Pros:**
  - Highly optimized for streaming events from databases (via Debezium CDC) or writing Kafka topics to S3/Iceberg.
* **Cons:**
  - Extra infrastructure to manage (Kafka Connect cluster).
  - Schema configuration can be complex and brittle for custom JSON models.

### 3. Custom PySpark & Python loaders
* **Pros:**
  - Single execution framework (Spark) for both ingestion and transformation.
  - Complete control over schema mapping, deduplication, and Iceberg table optimization (e.g. write options, partitioning).
  - Highly reproducible in a single local Docker Compose environment.
  - Easily instrumented with OpenLineage and custom logger libraries.
* **Cons:**
  - Requires writing and maintaining extraction and loading code.

## Decision
We will use **Custom PySpark & Python loaders** for our ingestion pipelines.

## Rationale
1. **Unified Engine:** Using PySpark for both Batch and Streaming allows us to write uniform ingestion logic. The Shopify API mock extracts JSON and writes to S3 via Spark; the CSV directory loader reads via Spark; and the POS Kafka stream utilizes PySpark Structured Streaming.
2. **Iceberg Catalog Control:** Direct PySpark integration with the Nessie Catalog enables us to execute DDL statements (e.g. `CREATE TABLE IF NOT EXISTS nessie.bronze.table USING iceberg`) and configure hidden partitioning directly from code.
3. **OpenLineage Compatibility:** Spark's OpenLineage listener automatically parses Spark query plans and pushes datasets and schemas directly to Marquez.

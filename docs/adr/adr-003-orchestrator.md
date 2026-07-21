# ADR 03: Orchestrator Selection

## Status
Approved

## Context
CartCo needs to orchestrate multiple batch pipelines (Shopify pulling, Distributor CSV processing, and Medallion layer promotions) with proper scheduling, failure retries, dependency management, and logging.

We need to choose an orchestrator:
1. **Apache Airflow**
2. **Dagster**
3. **Prefect**

## Alternatives Considered

### 1. Apache Airflow
* **Pros:**
  - The industry standard with a massive ecosystem of integrations (operators, hooks, providers).
  - Out-of-the-box support for OpenLineage (auto-instrumentation for Airflow and Spark).
  - Robust UI for task monitoring, log viewing, and manual retries.
  - Highly reliable scheduler.
* **Cons:**
  - Configuration overhead; requires spinning up a database, webserver, scheduler, and worker.
  - Historically code-heavy for dynamic configurations, though modern task-flow APIs have resolved this.

### 2. Dagster
* **Pros:**
  - Asset-oriented programming model (focuses on data assets rather than tasks).
  - Excellent developer experience with local testing utilities.
  - Native understanding of schemas and data lineage.
* **Cons:**
  - Smaller community and fewer out-of-the-box enterprise integrations compared to Airflow.
  - Steeper learning curve for teams transition from traditional DAG models.

### 3. Prefect
* **Pros:**
  - Modern, pythonic design (any python function can be a task/flow).
  - Dynamic scheduling and orchestration out-of-the-box.
* **Cons:**
  - Relies heavily on its cloud backend or complex self-hosted server configurations.
  - OpenLineage support is less mature compared to Airflow's built-in providers.

## Decision
We will use **Apache Airflow** as the central orchestrator.

## Rationale
1. **OpenLineage Integration:** Airflow 2.x comes with built-in OpenLineage support. This enables automatic tracing of lineage from Airflow DAGs to Marquez, which is critical for our observability requirement.
2. **Ecosystem & Community:** The vast array of pre-built integrations (e.g. SparkSubmitOperator, S3Hook, PostgresOperator) ensures rapid development.
3. **Enterprise Adoption:** Recruiters and engineering leads at targets like Razorpay, Flipkart, and PhonePe heavily prioritize Airflow expertise.

# CartCo Unified Commerce Lakehouse 🛒📈
### Medallion Lakehouse Architecture for a Multi-Channel Retailer

Welcome to the foundation of **CartCo's Data Platform**, a state-of-the-art **Medallion Lakehouse** engineered to ingest, clean, validate, and analyze retail sales across Shopify (web), physical stores (POS events via Kafka), and wholesale distributors (daily CSV drops).

This repository contains the complete IaC Docker environment, PySpark pipelines, Airflow DAGs, Great Expectations suites, C4 architecture diagrams, and a Streamlit visualization dashboard.

---

## 🛠️ Tech Stack & Key Choices

1. **Table Format: Apache Iceberg**
   - Supports ACID transactions, time-travel queries, hidden partitioning (prunes queries automatically), and clean schema evolution.
2. **Catalog: Project Nessie**
   - Acts as a "Git for Data", allowing branching, merging, and catalog-level transaction rollbacks.
3. **Storage: MinIO (S3 API Compatible)**
   - Local high-performance object storage backing our `bronze`, `silver`, `gold`, and `warehouse` zones.
4. **Compute Engine: Apache Spark (PySpark v3.5)**
   - Distributed batch processing for Shopify and distributor CSVs, and PySpark Structured Streaming for POS transactions.
5. **Orchestrator: Apache Airflow (v2.7)**
   - Schedules and coordinates all pipelines, with task-level dependency tracking.
6. **Observability: OpenLineage + Marquez**
   - Automatically parses Spark execution plans and Airflow DAGs to map and record dataset lineage in real time.
7. **Data Quality: Great Expectations**
   - Validates Silver layer schemas and asserts range checks (e.g., non-null IDs, unique keys, positive quantities, positive sales amounts).

---

## 📂 Project Directory Structure

```text
├── docker-compose.yml              # Multi-service stack (MinIO, Nessie, Spark, Airflow, Redpanda, Marquez)
├── README.md                       # Technical setup guide
├── docs/
│   ├── adr/                        # 5 Architectural Decision Records
│   │   ├── adr-001-table-format.md
│   │   ├── adr-002-catalog.md
│   │   ├── adr-003-orchestrator.md
│   │   ├── adr-004-ingestion-pattern.md
│   │   └── adr-005-schema-evolution-data-quality.md
│   └── architecture.md             # C4 Diagram views (Container and Component level)
├── docker/
│   ├── postgres/
│   │   └── init-db.sql             # SQL script to create Airflow & Marquez databases
│   └── airflow/
│       └── Dockerfile              # Custom Airflow image installing OpenJDK 17 + PySpark dependencies
├── src/
│   ├── generators/                 # Simulated multi-channel transaction generators
│   │   ├── shopify_generator.py
│   │   ├── distributor_csv_generator.py
│   │   └── pos_kafka_generator.py
│   ├── jobs/                       # PySpark pipeline jobs
│   │   ├── spark_helper.py         # Standardized Spark session builder for S3/Iceberg/Nessie
│   │   ├── bronze/
│   │   │   ├── ingest_shopify.py
│   │   │   ├── ingest_csv.py
│   │   │   └── ingest_pos.py
│   │   ├── silver/
│   │   │   ├── refine_silver.py    # Upsert conformed Silver tables
│   │   │   └── validate_silver.py  # Great Expectations assertions
│   │   └── gold/
│   │       └── refine_gold.py      # Business mart aggregations
│   ├── airflow/
│   │   └── dags/                   # Medallion orchestration DAGs
│   │       ├── dag_shopify_bronze.py
│   │       ├── dag_distributor_bronze.py
│   │       ├── dag_pos_bronze.py
│   │       ├── dag_silver_promotion.py
│   │       └── dag_gold_marts.py
│   └── dashboard/
│       └── app.py                  # Streamlit visualizer for Gold Mart statistics
└── data/                           # Landing zone directory mounts (mapped to Docker)
    ├── shopify_drops/
    ├── distributor_drops/
    └── pos_drops/
```

---

## 🚀 Setting Up the Stack (Local Execution)

### 1. Prerequisites
Ensure you have the following installed on your machine:
- **Docker & Docker Compose** (needed to spin up the local lakehouse)
- **Python 3.10+** (if you want to run generators and dashboard locally)

### 2. Booting up the Lakehouse
Spin up all database, catalog, streaming, compute, orchestrator, and lineage containers:
```bash
docker compose up -d --build
```
This builds our custom Airflow container (installing Java + PySpark) and triggers the MinIO bootstrap step to create the S3 buckets (`bronze`, `silver`, `gold`, `warehouse`).

### 3. Verify Container Health
Once booted, verify that the services are online via their Web UIs:
- **Apache Airflow Webserver:** [http://localhost:8089](http://localhost:8089) (Default Creds: `admin` / `admin` or automatic auto-login)
- **MinIO Console:** [http://localhost:9001](http://localhost:9001) (Creds: `admin` / `password123`)
- **Marquez Lineage UI:** [http://localhost:3000](http://localhost:3000)
- **Spark Master UI:** [http://localhost:8085](http://localhost:8085)
- **Redpanda Console:** [http://localhost:8080](http://localhost:8080)
- **Project Nessie Catalog API:** [http://localhost:19120](http://localhost:19120)

---

## 📈 Running the Data Pipeline

The pipeline flows from **Simulated Data Generators** -> **Bronze Ingest** -> **Silver Conform & Validate** -> **Gold Mart Aggregations**.

### 1. Generating Traffic (Ingestion landing drops)
To simulate live user orders and wholesale uploads, run our generator scripts. If you don't have Kafka/Redpanda running, the scripts automatically fall back to writing raw file dumps in our local landing folders:
```bash
# Generate Shopify API orders JSON drop
python src/generators/shopify_generator.py

# Generate Distributor CSV sales report
python src/generators/distributor_csv_generator.py

# Generate physical store POS events (publishes to Redpanda topic, falls back to JSON file)
python src/generators/pos_kafka_generator.py
```

### 2. Executing Airflow DAGs
Open the [Airflow Portal](http://localhost:8089) and trigger the following DAGs in order (or let them run on their schedules):
1. **`dag_shopify_bronze`** / **`dag_distributor_bronze`** / **`dag_pos_bronze`**: These extract files from our landing directories, add lineage tags and ingestion timestamps, write the raw tables into the Iceberg Bronze layer (`nessie.bronze.*`), and move processed files to an `archive/` folder.
2. **`dag_silver_promotion`**: This triggers the PySpark cleaning and deduplication job to conjoin the sources into a unified structure (`orders`, `order_items`, `products`). It then runs Great Expectations to ensure schemas and prices match parameters.
3. **`dag_gold_marts`**: Summarizes conformed data into daily revenue, inventory, and customer profile tables (`nessie.gold.*`).

---

## 🎯 Lineage & Quality Observability

- **OpenLineage Tracking**: Open Marquez at [http://localhost:3000](http://localhost:3000). You will see the auto-generated lineage map showing exactly how raw JSON/CSVs feed into `bronze.shopify_orders`, which flow through `refine_silver` to create the canonical `silver.orders` and final Gold datamarts.
- **Great Expectations Logging**: In Airflow, if a record has duplicate keys or a negative sale value, `dag_silver_promotion` will fail during the `validate_silver` step, raising a clear assertion error in the logs, protecting downstream analysts.

---

## 📊 Viewing the Analytics Dashboard

We have included a beautiful **Streamlit** dashboard to visualize the Gold Mart insights (gross revenues, sales-by-channel over time, best-selling SKUs, customer profiles).

Install dependencies and launch it locally:
```bash
pip install streamlit pandas
streamlit run src/dashboard/app.py
```
*(If Spark/MinIO is offline, the app dynamically generates in-memory mock datasets conforming to our Gold schemas so you can explore the user interface).*

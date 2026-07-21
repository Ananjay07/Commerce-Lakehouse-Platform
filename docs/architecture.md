# CartCo Unified Commerce Lakehouse C4 Architecture Model

This document describes the architectural layout of the CartCo Medallion Lakehouse platform using the C4 Model format (Container and Component views).

---

## 1. Container Diagram (System Level)

The Container Diagram illustrates the high-level boundaries of the data platform, showing how external systems feed data in, how the compute/storage engines are arranged, and how analysts/observability platforms interact with the stack.

```mermaid
graph TB
    %% External Systems / Sources
    subgraph "External Channels & Sources"
        shopify[("Shopify eCommerce<br/>(GraphQL Orders JSON)")]
        pos_stores[("Physical Stores POS<br/>(Store Txn Events)")]
        distributors[("Distributors<br/>(Daily CSV drops via SFTP)")]
    end

    %% Ingestion Brokers
    subgraph "Ingestion Broker Layer"
        redpanda["Redpanda / Kafka Broker<br/>(POS Topic: pos-transactions)"]
        local_fs["Local landing storage<br/>(distributor_drops/ & shopify_drops/)"]
    end

    %% Compute & Orchestration
    subgraph "Compute & Orchestration Layer"
        airflow["Apache Airflow v2.7<br/>(Scheduler & Web UI)"]
        spark["Apache Spark / PySpark v3.5<br/>(Master & Workers)"]
        ge_framework["Great Expectations<br/>(Validation engine)"]
    end

    %% Catalog & Metadata
    subgraph "Catalog & Version Control"
        nessie["Project Nessie Catalog<br/>(REST Server / git-like branches)"]
    end

    %% Storage Layer
    subgraph "Lakehouse Storage (MinIO S3)"
        subgraph "Iceberg Medallion Tables"
            bronze_bucket[("s3a://bronze/<br/>Raw Partitioned Tables")]
            silver_bucket[("s3a://silver/<br/>Cleaned Canonical Tables")]
            gold_bucket[("s3a://gold/<br/>Aggregated Business Marts")]
        end
    end

    %% Observability & Consumption
    subgraph "Observability & Consumption Layer"
        marquez["Marquez + OpenLineage<br/>(Metadata / Lineage UI)"]
        postgres[("Postgres DB<br/>(Airflow & Marquez metadata)")]
        bi_tools["Analytics & BI Clients<br/>(Streamlit / Trino / DuckDB)"]
    end

    %% Flows
    shopify -->|JSON Pull| local_fs
    distributors -->|CSV Drops| local_fs
    pos_stores -->|Publish Events| redpanda
    
    airflow -->|Trigger PySpark ETL| spark
    spark -->|Pull raw drops| local_fs
    spark -->|Pull stream events| redpanda
    
    spark -->|Write / Update metadata| nessie
    spark -->|Write Parquet data files| bronze_bucket
    spark -->|Write Cleaned / Deduplicated data| silver_bucket
    spark -->|Read Silver & Write Aggregated Marts| gold_bucket
    
    spark -.->|Register Spark execution plan| marquez
    airflow -.->|Register task lineage| marquez
    
    spark -->|Quality Validation gate| ge_framework
    ge_framework -->|Assert schema & quality rules| silver_bucket
    
    airflow -.->|Read/Write state| postgres
    marquez -.->|Persist Lineage metadata| postgres
    
    bi_tools -->|Read Gold Marts| gold_bucket
    bi_tools -->|Query Catalog| nessie
```

---

## 2. Component Diagram (Transformation Pipelines)

The Component Diagram zooms into the **Compute & Orchestration Container** to show how specific jobs, operators, and validation suites interact to move data across the Bronze, Silver, and Gold layers.

```mermaid
graph LR
    subgraph "Ingestion Components (Bronze Stage)"
        job_shopify["ingest_shopify.py<br/>(Extract JSON, append metadata)"]
        job_csv["ingest_csv.py<br/>(Parse CSVs, infer schemas)"]
        job_pos["ingest_pos.py<br/>(Kafka Stream / batch fallback)"]
    end

    subgraph "Nessie Catalog / Iceberg Tables"
        table_shp[("bronze.shopify_orders")]
        table_csv[("bronze.distributor_sales")]
        table_pos[("bronze.pos_transactions")]
        
        table_orders[("silver.orders")]
        table_items[("silver.order_items")]
        table_products[("silver.products")]
        
        mart_rev[("gold.daily_revenue_channel")]
        mart_inv[("gold.inventory_turnover")]
        mart_cust[("gold.customer_360")]
    end

    subgraph "Transformation Components (Silver Stage)"
        refine_silver["refine_silver.py<br/>(Clean, Deduplicate, Merge)"]
        validate_silver["validate_silver.py<br/>(Great Expectations assertions)"]
    end

    subgraph "Aggregation Components (Gold Stage)"
        refine_gold["refine_gold.py<br/>(Daily Marts, Customer 360, SKU performance)"]
    end

    %% Mappings
    job_shopify -->|Write Iceberg append| table_shp
    job_csv -->|Write Iceberg append| table_csv
    job_pos -->|Write Stream append| table_pos

    table_shp --> refine_silver
    table_csv --> refine_silver
    table_pos --> refine_silver

    refine_silver -->|Merge SQL| table_orders
    refine_silver -->|Merge SQL| table_items
    refine_silver -->|Merge SQL| table_products

    table_orders --> validate_silver
    table_items --> validate_silver

    validate_silver -->|Validate rules| refine_gold
    
    refine_gold -->|Aggregates| mart_rev
    refine_gold -->|Aggregates| mart_inv
    refine_gold -->|Aggregates| mart_cust
```

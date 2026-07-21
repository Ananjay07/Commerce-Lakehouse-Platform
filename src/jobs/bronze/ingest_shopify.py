import os
import shutil
import sys
from datetime import datetime
from pyspark.sql.functions import input_file_name, current_timestamp, date_format, lit
from pyspark.sql.types import StructType

# Add parent directory to path to import spark_helper and contract_validator
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from jobs.spark_helper import get_spark_session
from contracts.contract_validator import validate_shopify_file

LANDING_DIR = "/opt/airflow/data/shopify_drops"
ARCHIVE_DIR = "/opt/airflow/data/shopify_drops/archive"
QUARANTINE_DIR = "/opt/airflow/data/shopify_drops/quarantine"

# Fallbacks for running outside of docker
if not os.path.exists("/opt/airflow"):
    LANDING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/shopify_drops"))
    ARCHIVE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/shopify_drops/archive"))
    QUARANTINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/shopify_drops/quarantine"))

def run_ingest():
    spark = get_spark_session("Ingest-Shopify-Bronze")
    
    # Check for JSON files in landing zone (ignoring directories like archive)
    json_files = [f for f in os.listdir(LANDING_DIR) if f.endswith(".json")]
    
    if not json_files:
        print("No new Shopify files to ingest.")
        spark.stop()
        return
        
    print(f"Found {len(json_files)} Shopify files to ingest: {json_files}")
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    # Process files
    for file_name in json_files:
        file_path = os.path.join(LANDING_DIR, file_name)
        
        # 1. Validate incoming data against Schema Contract
        try:
            validate_shopify_file(file_path)
        except Exception as e:
            print(f"[FAIL] Data Contract Violation for {file_name}: {e}")
            os.makedirs(QUARANTINE_DIR, exist_ok=True)
            quarantine_path = os.path.join(QUARANTINE_DIR, file_name)
            shutil.move(file_path, quarantine_path)
            print(f"Quarantined non-compliant file {file_name} to {quarantine_path}")
            continue
        
        # 2. Load JSON file
        # Shopify JSON contains "data.orders.edges" containing the array of order nodes
        raw_df = spark.read.option("multiline", "true").json(file_path)
        
        # Extract the array of order nodes
        # If the structure doesn't match, log a warning
        if "data" not in raw_df.columns:
            print(f"Skipping file {file_name}: Unrecognized schema structure.")
            continue
            
        # Explode/extract the order nodes
        # Raw structure: data -> orders -> edges -> list of { node: { id, name ... } }
        from pyspark.sql.functions import explode
        
        exploded_df = raw_df.select(
            explode("data.orders.edges").alias("order_edge")
        )
        
        # Extract the node fields
        orders_df = exploded_df.select(
            "order_edge.node.id",
            "order_edge.node.name",
            "order_edge.node.createdAt",
            "order_edge.node.totalPriceSet",
            "order_edge.node.subtotalPriceSet",
            "order_edge.node.totalTaxSet",
            "order_edge.node.email",
            "order_edge.node.customer",
            "order_edge.node.lineItems",
            "order_edge.node.sourceName",
            "order_edge.node.tags"
        )
        
        # Add metadata columns
        bronze_df = orders_df \
            .withColumn("_ingest_file_name", lit(file_name)) \
            .withColumn("_ingest_time", current_timestamp()) \
            .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))
            
        # Create Iceberg Schema / Table if not exists
        spark.sql("CREATE SCHEMA IF NOT EXISTS nessie.bronze")
        
        table_name = "nessie.bronze.shopify_orders"
        
        # Write to Iceberg
        bronze_df.write \
            .format("iceberg") \
            .mode("append") \
            .option("check-ordering", "false") \
            .save(table_name)
            
        print(f"Successfully ingested {file_name} into {table_name}")
        
        # Archive file
        archive_path = os.path.join(ARCHIVE_DIR, file_name)
        shutil.move(file_path, archive_path)
        print(f"Archived {file_name} to {archive_path}")
        
    spark.stop()

if __name__ == "__main__":
    run_ingest()

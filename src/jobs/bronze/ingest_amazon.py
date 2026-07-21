import os
import shutil
import sys
from datetime import datetime
from pyspark.sql.functions import explode, lit, current_timestamp, date_format
from pyspark.sql.types import StructType

# Add parent directory to path to import spark_helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from jobs.spark_helper import get_spark_session

LANDING_DIR = "/opt/airflow/data/amazon_drops"
ARCHIVE_DIR = "/opt/airflow/data/amazon_drops/archive"

# Fallbacks for running outside of docker
if not os.path.exists("/opt/airflow"):
    LANDING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/amazon_drops"))
    ARCHIVE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/amazon_drops/archive"))

def run_ingest():
    spark = get_spark_session("Ingest-Amazon-Bronze")
    
    # Check for JSON files in landing zone (ignoring archive directory)
    json_files = [f for f in os.listdir(LANDING_DIR) if f.endswith(".json")]
    
    if not json_files:
        print("No new Amazon files to ingest.")
        spark.stop()
        return
        
    print(f"Found {len(json_files)} Amazon files to ingest: {json_files}")
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    # Process files
    for file_name in json_files:
        file_path = os.path.join(LANDING_DIR, file_name)
        
        # Load JSON file
        # Amazon JSON contains "Orders" containing the array of orders
        raw_df = spark.read.option("multiline", "true").json(file_path)
        
        # Check if schema contains "Orders"
        if "Orders" not in raw_df.columns:
            print(f"Skipping file {file_name}: Unrecognized schema structure (no 'Orders' column).")
            continue
            
        # Explode the Orders array
        exploded_df = raw_df.select(
            explode("Orders").alias("order")
        )
        
        # Select individual order fields to load to Bronze
        orders_df = exploded_df.select(
            "order.AmazonOrderId",
            "order.SellerOrderId",
            "order.PurchaseDate",
            "order.LastUpdateDate",
            "order.OrderStatus",
            "order.FulfillmentChannel",
            "order.SalesChannel",
            "order.OrderTotal",
            "order.NumberOfItemsShipped",
            "order.NumberOfItemsUnshipped",
            "order.PaymentMethod",
            "order.BuyerEmail",
            "order.BuyerInfo",
            "order.OrderItems"
        )
        
        # Add metadata columns
        bronze_df = orders_df \
            .withColumn("_ingest_file_name", lit(file_name)) \
            .withColumn("_ingest_time", current_timestamp()) \
            .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))
            
        # Create Iceberg Schema / Table if not exists
        spark.sql("CREATE SCHEMA IF NOT EXISTS nessie.bronze")
        
        table_name = "nessie.bronze.amazon_sales"
        
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

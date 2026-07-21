import os
import shutil
import sys
from datetime import datetime
from pyspark.sql.functions import col, from_json, lit, current_timestamp, date_format, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

# Add parent directory to path to import spark_helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from jobs.spark_helper import get_spark_session

LANDING_DIR = "/opt/airflow/data/pos_drops"
ARCHIVE_DIR = "/opt/airflow/data/pos_drops/archive"
CHECKPOINT_DIR = "/opt/airflow/data/pos_drops/checkpoints"

# Fallbacks for running outside of docker
if not os.path.exists("/opt/airflow"):
    LANDING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/pos_drops"))
    ARCHIVE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/pos_drops/archive"))
    CHECKPOINT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/pos_drops/checkpoints"))

# Define schemas for POS Transactions JSON
item_schema = StructType([
    StructField("sku", StringType(), True),
    StructField("title", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("amount", DoubleType(), True)
])

pos_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("cashier_id", StringType(), True),
    StructField("items", ArrayType(item_schema), True),
    StructField("subtotal_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("customer_loyalty_id", StringType(), True)
])

def check_kafka_connection(bootstrap_servers):
    """Simple connection check before initiating stream."""
    try:
        from confluent_kafka import Consumer
        c = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'test-group',
            'socket.timeout.ms': 1000,
            'message.timeout.ms': 1000
        })
        c.list_topics(timeout=1.0)
        return True
    except Exception:
        return False

def run_streaming_ingest(spark, kafka_servers):
    print(f"Starting Spark Structured Streaming ingest from Kafka: {kafka_servers}")
    
    # Read stream from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "pos-transactions") \
        .option("startingOffsets", "earliest") \
        .load()
        
    # Cast value payload to string, parse json schema
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), pos_schema).alias("data")) \
        .select("data.*")
        
    # Add metadata columns
    bronze_stream = parsed_stream \
        .withColumn("_ingest_file_name", lit("kafka-stream")) \
        .withColumn("_ingest_time", current_timestamp()) \
        .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))
        
    spark.sql("CREATE SCHEMA IF NOT EXISTS nessie.bronze")
    
    table_name = "nessie.bronze.pos_transactions"
    
    # Ensure Iceberg table is initialized (avoids schema mismatches in streams)
    # We create table if it does not exist using an empty dataframe structure
    try:
        spark.sql(f"SELECT * FROM {table_name} LIMIT 1")
    except Exception:
        # Create empty table initialized with df schema
        print(f"Initializing target Iceberg table {table_name}")
        # Build a dummy row to create the table structure
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), bronze_stream.schema)
        empty_df.write \
            .format("iceberg") \
            .mode("append") \
            .option("check-ordering", "false") \
            .save(table_name)
            
    # Write stream to Iceberg
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    
    query = bronze_stream.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .toTable(table_name)
        
    print("Stream running. Waiting for termination...")
    query.awaitTermination()

def run_batch_ingest(spark):
    print("Running in Batch/Directory Fallback Ingestion Mode...")
    json_files = [f for f in os.listdir(LANDING_DIR) if f.endswith(".json")]
    
    if not json_files:
        print("No new POS transaction files to ingest.")
        return
        
    print(f"Found {len(json_files)} POS transaction files to ingest: {json_files}")
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    for file_name in json_files:
        file_path = os.path.join(LANDING_DIR, file_name)
        
        # Read JSON file (which is a list of transactions)
        raw_df = spark.read.option("multiline", "true").json(file_path)
        
        # Structuring columns to match the canonical stream schema
        # In batch mode, we can read it directly into the schema.
        # But wait! If the file is structured as a list, Spark parses it as a list/array of struct.
        # Let's handle list parsing:
        # If it's a list, the columns will just be the fields if Spark parsed it as multiple rows.
        # Spark's json reader parses a JSON array of objects as multiple rows out-of-the-box!
        # So raw_df already has transaction_id, store_id, items, etc.
        # Let's enforce the target schema to align datatypes.
        typed_df = spark.read.option("multiline", "true").schema(pos_schema).json(file_path)
        
        # Add metadata columns
        bronze_df = typed_df \
            .withColumn("_ingest_file_name", lit(file_name)) \
            .withColumn("_ingest_time", current_timestamp()) \
            .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))
            
        # Create Iceberg Schema / Table if not exists
        spark.sql("CREATE SCHEMA IF NOT EXISTS nessie.bronze")
        
        table_name = "nessie.bronze.pos_transactions"
        
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

def main():
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9094")
    streaming_flag = len(sys.argv) > 1 and sys.argv[1] == "--streaming"
    
    spark = get_spark_session("Ingest-POS-Bronze")
    
    if streaming_flag and check_kafka_connection("localhost:9092" if kafka_servers == "redpanda:9094" and not os.path.exists("/opt/airflow") else kafka_servers):
        try:
            run_streaming_ingest(spark, kafka_servers)
        except KeyboardInterrupt:
            print("Streaming job halted by user.")
        finally:
            spark.stop()
    else:
        if streaming_flag:
            print("Kafka not reachable. Falling back to batch directory mode...")
        run_batch_ingest(spark)
        spark.stop()

if __name__ == "__main__":
    main()

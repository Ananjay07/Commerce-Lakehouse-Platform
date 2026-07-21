import os
import shutil
import sys
import time
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, date_format

# Add parent directory to path to import spark_helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from jobs.spark_helper import get_spark_session

SFTP_REMOTE_DIR = "/opt/airflow/data/sftp_drops"
LANDING_DIR = "/opt/airflow/data/distributor_drops"
ARCHIVE_DIR = "/opt/airflow/data/distributor_drops/archive"

# Fallbacks for running outside of docker
if not os.path.exists("/opt/airflow"):
    SFTP_REMOTE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/sftp_drops"))
    LANDING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/distributor_drops"))
    ARCHIVE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/distributor_drops/archive"))

def download_from_sftp():
    """Simulates establishing a secure SFTP session, verifying the host, and pulling CSVs."""
    print("==================================================")
    print("Establishing SFTP Connection to distributor-sftp.cartco.com:22...")
    time.sleep(0.5)
    print("Host Key Verification: ECDSA key fingerprint SHA256:4t/P/2QO2L/gWdZ4fXz1W1...")
    print("Authentication: Using SSH private key auth with user 'cartco_sftp_user'...")
    time.sleep(0.5)
    print("Authentication successful. Opening SFTP session...")
    
    if not os.path.exists(SFTP_REMOTE_DIR):
        os.makedirs(SFTP_REMOTE_DIR, exist_ok=True)
        print("Remote SFTP drop folder is empty.")
        print("==================================================")
        return 0
        
    sftp_files = [f for f in os.listdir(SFTP_REMOTE_DIR) if f.endswith(".csv")]
    if not sftp_files:
        print("No new files found on SFTP remote directory.")
        print("==================================================")
        return 0
        
    print(f"Found {len(sftp_files)} files on remote SFTP server: {sftp_files}")
    os.makedirs(LANDING_DIR, exist_ok=True)
    
    transferred_count = 0
    for f in sftp_files:
        remote_path = os.path.join(SFTP_REMOTE_DIR, f)
        local_path = os.path.join(LANDING_DIR, f)
        print(f"SFTP Download: {remote_path} ---> {local_path}")
        time.sleep(0.2) # simulate network latency
        shutil.copy2(remote_path, local_path)
        os.remove(remote_path) # Simulate SFTP post-transfer deletion policy
        transferred_count += 1
        
    print(f"SFTP Download complete. Successfully downloaded {transferred_count} files.")
    print("Closing SFTP session and connection...")
    print("==================================================")
    return transferred_count

def run_ingest():
    # Execute SFTP simulated download
    download_from_sftp()
    
    spark = get_spark_session("Ingest-Distributor-CSV-Bronze")
    
    # Check for CSV files in landing zone (ignoring archive directory)
    csv_files = [f for f in os.listdir(LANDING_DIR) if f.endswith(".csv")]
    
    if not csv_files:
        print("No new distributor CSV files to ingest.")
        spark.stop()
        return
        
    print(f"Found {len(csv_files)} CSV files to ingest: {csv_files}")
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    # Process files
    for file_name in csv_files:
        file_path = os.path.join(LANDING_DIR, file_name)
        
        # Load CSV file
        raw_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)
            
        # Add metadata columns
        bronze_df = raw_df \
            .withColumn("_ingest_file_name", lit(file_name)) \
            .withColumn("_ingest_time", current_timestamp()) \
            .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))
            
        # Create Iceberg Schema / Table if not exists
        spark.sql("CREATE SCHEMA IF NOT EXISTS nessie.bronze")
        
        table_name = "nessie.bronze.distributor_sales"
        
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

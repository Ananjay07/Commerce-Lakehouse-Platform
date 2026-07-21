import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# Define paths
FS_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.abspath(os.path.join(FS_DIR, "../../data/feature_store"))
os.makedirs(DATA_DIR, exist_ok=True)

CUSTOMER_PARQUET_PATH = os.path.join(DATA_DIR, "gold_customer_360.parquet")
PRODUCT_PARQUET_PATH = os.path.join(DATA_DIR, "gold_inventory_turnover.parquet")

def generate_offline_parquet_files():
    """
    Generates synthetic Gold-layer snapshot parquet files representing
    our Lakehouse Gold tables, decorated with timestamps required by Feast.
    """
    print("Generating offline Parquet files from Gold tables...")
    now = datetime.utcnow()
    
    # 1. Customer 360 features
    cust_data = {
        "customer_id": [f"CUST-{i:03d}" for i in range(1, 11)],
        "customer_email": [f"user{i}@example.com" for i in range(1, 11)],
        "total_orders": [i * 3 for i in range(1, 11)],
        "total_spend": [float(i * 1250.50) for i in range(1, 11)],
        "preferred_channel": ["shopify" if i % 2 == 0 else "amazon" for i in range(1, 11)],
        "event_timestamp": [now - timedelta(hours=i) for i in range(1, 11)],
        "created_timestamp": [now for _ in range(10)]
    }
    df_cust = pd.DataFrame(cust_data)
    df_cust.to_parquet(CUSTOMER_PARQUET_PATH, index=False)
    print(f"  Saved customer features to: {CUSTOMER_PARQUET_PATH}")
    
    # 2. Product/Inventory features
    prod_data = {
        "sku": ["SKU-TSHIRT-01", "SKU-JEANS-02", "SKU-HOODIE-03", "SKU-SOCKS-04", "SKU-JACKET-05"],
        "product_title": ["Classic White T-Shirt", "Slim Fit Blue Jeans", "Fleece Hoodie Black", "Ankle Socks (3-Pack)", "Premium Denim Jacket"],
        "units_sold": [150, 80, 45, 300, 20],
        "total_sales_amount": [74850.00, 103920.00, 85455.00, 89700.00, 49980.00],
        "event_timestamp": [now - timedelta(days=1) for _ in range(5)],
        "created_timestamp": [now for _ in range(5)]
    }
    df_prod = pd.DataFrame(prod_data)
    df_prod.to_parquet(PRODUCT_PARQUET_PATH, index=False)
    print(f"  Saved product features to: {PRODUCT_PARQUET_PATH}")

def run_materialization():
    try:
        from feast import FeatureStore
    except ImportError:
        print("\n[WARNING] Feast is not installed. Skipping materialization logic.")
        print("To run this, install feast first: pip install feast\n")
        return
        
    print("\nInitializing Feast Feature Store...")
    # Add FS_DIR to path to make sure features can be imported
    sys.path.append(FS_DIR)
    
    store = FeatureStore(repo_path=FS_DIR)
    
    print("Running 'feast apply' equivalent...")
    from features import customer, product, customer_features_view, product_features_view
    store.apply([customer, product, customer_features_view, product_features_view])
    print("Feature definitions registered successfully in Registry.")
    
    print("Materializing features from offline store to SQLite online store...")
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=90)
    
    store.materialize(start_date, end_date)
    print("Materialization complete! Online store is populated.")

if __name__ == "__main__":
    generate_offline_parquet_files()
    run_materialization()

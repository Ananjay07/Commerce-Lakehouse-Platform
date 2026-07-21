import os
import sys

FS_DIR = os.path.abspath(os.path.dirname(__file__))

def query_online_features():
    try:
        from feast import FeatureStore
    except ImportError:
        print("\n[WARNING] Feast is not installed. Cannot query features.")
        print("Install it using: pip install feast\n")
        return

    # Load store
    store = FeatureStore(repo_path=FS_DIR)
    
    # Query customer features for serving
    entity_rows = [
        {"customer_id": "CUST-002"},
        {"customer_id": "CUST-005"},
    ]
    
    features_to_fetch = [
        "customer_features:total_orders",
        "customer_features:total_spend",
        "customer_features:preferred_channel"
    ]
    
    print("Querying online features from SQLite store...")
    response = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows
    ).to_dict()
    
    # Print results
    for i in range(len(entity_rows)):
        cust_id = response["customer_id"][i]
        orders = response["total_orders"][i]
        spend = response["total_spend"][i]
        channel = response["preferred_channel"][i]
        print(f"\nCustomer: {cust_id}")
        print(f"  - Total Orders: {orders}")
        print(f"  - Total Spend:  INR {spend:,.2f}")
        print(f"  - Preferred Channel: {channel}")

if __name__ == "__main__":
    query_online_features()

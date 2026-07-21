import os
import sys

# Add parent directory to path to import spark_helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from jobs.spark_helper import get_spark_session

def validate_orders(spark):
    print("Validating silver.orders table...")
    try:
        df = spark.read.format("iceberg").load("nessie.silver.orders")
    except Exception as e:
        print(f"Error loading silver.orders: {e}")
        raise
        
    total_count = df.count()
    if total_count == 0:
        print("Warning: silver.orders is empty. Skipping validation.")
        return
        
    from great_expectations.dataset import SparkDFDataset
    ge_df = SparkDFDataset(df)
    
    # 1. order_id must not be null
    r1 = ge_df.expect_column_values_to_not_be_null("order_id")
    # 2. channel must be in canonical set
    r2 = ge_df.expect_column_values_to_be_in_set("channel", ["shopify", "pos", "distributor"])
    # 3. total_amount must be greater than or equal to 0
    r3 = ge_df.expect_column_values_to_be_greater_than_or_equal_to("total_amount", 0.0)
    # 4. created_at must not be null
    r4 = ge_df.expect_column_values_to_not_be_null("created_at")
    
    # Check uniqueness of order_id (critical)
    unique_count = df.select("order_id").distinct().count()
    is_unique = (unique_count == total_count)
    
    print(f"Orders Validation Results:")
    print(f"  - Non-null Order ID: {r1.success} (Failed rows: {r1.result.get('unexpected_count', 0)})")
    print(f"  - Valid Channels: {r2.success} (Failed rows: {r2.result.get('unexpected_count', 0)})")
    print(f"  - Non-negative Total Amount: {r3.success} (Failed rows: {r3.result.get('unexpected_count', 0)})")
    print(f"  - Non-null Creation Timestamp: {r4.success} (Failed rows: {r4.result.get('unexpected_count', 0)})")
    print(f"  - Unique Order ID Constraint: {is_unique} ({unique_count} unique vs {total_count} total)")
    
    # Critical failures check
    if not (r1.success and r2.success and r3.success and r4.success and is_unique):
        raise ValueError("Critical quality checks failed on silver.orders!")
        
    print("silver.orders validation passed successfully.")

def validate_order_items(spark):
    print("Validating silver.order_items table...")
    try:
        df = spark.read.format("iceberg").load("nessie.silver.order_items")
    except Exception as e:
        print(f"Error loading silver.order_items: {e}")
        raise
        
    total_count = df.count()
    if total_count == 0:
        print("Warning: silver.order_items is empty. Skipping validation.")
        return
        
    from great_expectations.dataset import SparkDFDataset
    ge_df = SparkDFDataset(df)
    
    # 1. order_item_id must not be null
    r1 = ge_df.expect_column_values_to_not_be_null("order_item_id")
    # 2. order_id must not be null
    r2 = ge_df.expect_column_values_to_not_be_null("order_id")
    # 3. sku must not be null
    r3 = ge_df.expect_column_values_to_not_be_null("sku")
    # 4. quantity must be positive
    r4 = ge_df.expect_column_values_to_be_greater_than("quantity", 0)
    # 5. unit_price must not be negative
    r5 = ge_df.expect_column_values_to_be_greater_than_or_equal_to("unit_price", 0.0)
    
    # Check uniqueness of order_item_id
    unique_count = df.select("order_item_id").distinct().count()
    is_unique = (unique_count == total_count)
    
    print(f"Order Items Validation Results:")
    print(f"  - Non-null Item ID: {r1.success}")
    print(f"  - Non-null Order ID: {r2.success}")
    print(f"  - Non-null SKU: {r3.success}")
    print(f"  - Positive Quantities: {r4.success}")
    print(f"  - Non-negative Price: {r5.success}")
    print(f"  - Unique Item ID: {is_unique}")
    
    # Critical failures check
    if not (r1.success and r2.success and r3.success and r4.success and r5.success and is_unique):
        raise ValueError("Critical quality checks failed on silver.order_items!")
        
    print("silver.order_items validation passed successfully.")

def main():
    spark = get_spark_session("Validate-Silver-Data-Quality")
    try:
        validate_orders(spark)
        validate_order_items(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

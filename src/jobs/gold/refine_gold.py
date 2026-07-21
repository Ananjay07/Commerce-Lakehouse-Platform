import os
import sys
from pyspark.sql.functions import col, count, sum, avg, min, max, date_format, current_timestamp, first

# Add parent directory to path to import spark_helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from jobs.spark_helper import get_spark_session

def create_gold_schemas(spark):
    """Creates the Gold schema and tables if they do not exist."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS nessie.gold")
    
    # Daily Revenue and Channel Performance Mart
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.gold.daily_revenue_channel (
        sales_date STRING,
        channel STRING,
        order_count LONG,
        gross_revenue DOUBLE,
        tax_collected DOUBLE,
        average_order_value DOUBLE,
        _processed_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (channel)
    """)
    
    # Inventory and SKU Performance Mart
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.gold.inventory_turnover (
        sales_date STRING,
        sku STRING,
        product_title STRING,
        units_sold LONG,
        total_sales_amount DOUBLE,
        _processed_at TIMESTAMP
    ) USING iceberg
    """)
    
    # Customer 360 Profile Mart
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.gold.customer_360 (
        customer_id STRING,
        customer_email STRING,
        first_purchase_date TIMESTAMP,
        last_purchase_date TIMESTAMP,
        total_orders LONG,
        total_spend DOUBLE,
        preferred_channel STRING,
        _processed_at TIMESTAMP
    ) USING iceberg
    """)

def compute_daily_revenue_channel(spark):
    print("Computing Daily Revenue & Channel Performance...")
    orders = spark.read.format("iceberg").load("nessie.silver.orders")
    
    daily_revenue = orders.groupBy(
        date_format("created_at", "yyyy-MM-dd").alias("sales_date"),
        "channel"
    ).agg(
        count("order_id").alias("order_count"),
        sum("total_amount").alias("gross_revenue"),
        sum("tax_amount").alias("tax_collected"),
        avg("total_amount").alias("average_order_value")
    ).select(
        "sales_date",
        "channel",
        "order_count",
        col("gross_revenue").cast("double"),
        col("tax_collected").cast("double"),
        col("average_order_value").cast("double"),
        current_timestamp().alias("_processed_at")
    )
    
    # Save daily revenue (overwrite/replace partitions to be idempotent)
    daily_revenue.createOrReplaceTempView("new_daily_revenue")
    spark.sql("""
    MERGE INTO nessie.gold.daily_revenue_channel t
    USING new_daily_revenue s
    ON t.sales_date = s.sales_date AND t.channel = s.channel
    WHEN MATCHED THEN UPDATE SET
        t.order_count = s.order_count,
        t.gross_revenue = s.gross_revenue,
        t.tax_collected = s.tax_collected,
        t.average_order_value = s.average_order_value,
        t._processed_at = s._processed_at
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Daily Revenue compute done.")

def compute_inventory_turnover(spark):
    print("Computing Inventory Turnover...")
    orders = spark.read.format("iceberg").load("nessie.silver.orders")
    order_items = spark.read.format("iceberg").load("nessie.silver.order_items")
    
    # Join items with orders to get date
    joined = order_items.join(orders, "order_id", "inner")
    
    inventory = joined.groupBy(
        date_format("created_at", "yyyy-MM-dd").alias("sales_date"),
        "sku"
    ).agg(
        first("title").alias("product_title"),
        sum("quantity").alias("units_sold"),
        sum("total_amount").alias("total_sales_amount")
    ).select(
        "sales_date",
        "sku",
        "product_title",
        col("units_sold").cast("long"),
        col("total_sales_amount").cast("double"),
        current_timestamp().alias("_processed_at")
    )
    
    inventory.createOrReplaceTempView("new_inventory")
    spark.sql("""
    MERGE INTO nessie.gold.inventory_turnover t
    USING new_inventory s
    ON t.sales_date = s.sales_date AND t.sku = s.sku
    WHEN MATCHED THEN UPDATE SET
        t.product_title = s.product_title,
        t.units_sold = s.units_sold,
        t.total_sales_amount = s.total_sales_amount,
        t._processed_at = s._processed_at
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Inventory Turnover compute done.")

def compute_customer_360(spark):
    print("Computing Customer 360 Profiles...")
    orders = spark.read.format("iceberg").load("nessie.silver.orders")
    
    # Exclude null customer IDs
    valid_customers = orders.filter(col("customer_id").isNotNull())
    
    customer_profiles = valid_customers.groupBy("customer_id").agg(
        first("customer_email", ignorenulls=True).alias("customer_email"),
        min("created_at").alias("first_purchase_date"),
        max("created_at").alias("last_purchase_date"),
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_spend"),
        first("channel").alias("preferred_channel") # Simple proxy for preferred channel
    ).select(
        "customer_id",
        "customer_email",
        "first_purchase_date",
        "last_purchase_date",
        col("total_orders").cast("long"),
        col("total_spend").cast("double"),
        "preferred_channel",
        current_timestamp().alias("_processed_at")
    )
    
    customer_profiles.createOrReplaceTempView("new_customer_360")
    spark.sql("""
    MERGE INTO nessie.gold.customer_360 t
    USING new_customer_360 s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET
        t.customer_email = s.customer_email,
        t.first_purchase_date = s.first_purchase_date,
        t.last_purchase_date = s.last_purchase_date,
        t.total_orders = s.total_orders,
        t.total_spend = s.total_spend,
        t.preferred_channel = s.preferred_channel,
        t._processed_at = s._processed_at
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Customer 360 compute done.")

def main():
    spark = get_spark_session("Compute-Gold-Marts")
    
    create_gold_schemas(spark)
    
    try:
        compute_daily_revenue_channel(spark)
    except Exception as e:
        print(f"Failed to compute Daily Revenue Channel: {e}")
        
    try:
        compute_inventory_turnover(spark)
    except Exception as e:
        print(f"Failed to compute Inventory Turnover: {e}")
        
    try:
        compute_customer_360(spark)
    except Exception as e:
        print(f"Failed to compute Customer 360: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()

import os
import sys
from pyspark.sql.functions import col, explode, lit, concat, min, sum, first, current_timestamp, date_format, sha2

# Add parent directory to path to import spark_helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from jobs.spark_helper import get_spark_session

def create_silver_schemas(spark):
    """Creates the Silver schema and tables if they do not exist."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS nessie.silver")
    
    # Silver Orders
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.orders (
        order_id STRING,
        order_number STRING,
        channel STRING,
        created_at TIMESTAMP,
        customer_id STRING,
        customer_email STRING,
        currency STRING,
        subtotal_amount DOUBLE,
        tax_amount DOUBLE,
        total_amount DOUBLE,
        payment_method STRING,
        payment_status STRING,
        region STRING,
        _processed_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (channel)
    """)
    
    # Silver Order Items
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.order_items (
        order_item_id STRING,
        order_id STRING,
        sku STRING,
        title STRING,
        quantity INT,
        unit_price DOUBLE,
        total_amount DOUBLE,
        _processed_at TIMESTAMP
    ) USING iceberg
    """)
    
    # Silver Products Reference
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.products (
        sku STRING,
        title STRING,
        retail_price DOUBLE,
        _processed_at TIMESTAMP
    ) USING iceberg
    """)

def process_shopify_to_silver(spark):
    # Check if Bronze shopify table exists/has data
    try:
        shopify_bronze = spark.read.format("iceberg").load("nessie.bronze.shopify_orders")
        if shopify_bronze.count() == 0:
            return None, None
    except Exception as e:
        print(f"Shopify Bronze table not available: {e}")
        return None, None
        
    # Deduplicate Bronze by id, keeping latest _ingest_time
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("id").orderBy(col("_ingest_time").desc())
    deduped_shopify = shopify_bronze.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
    
    # Transform to Silver Orders
    orders_df = deduped_shopify.select(
        col("id").alias("order_id"),
        col("name").alias("order_number"),
        lit("shopify").alias("channel"),
        col("createdAt").cast("timestamp").alias("created_at"),
        col("customer.id").alias("customer_id"),
        col("email").alias("customer_email"),
        col("totalPriceSet.presentmentMoney.currencyCode").alias("currency"),
        col("subtotalPriceSet.presentmentMoney.amount").cast("double").alias("subtotal_amount"),
        col("totalTaxSet.presentmentMoney.amount").cast("double").alias("tax_amount"),
        col("totalPriceSet.presentmentMoney.amount").cast("double").alias("total_amount"),
        col("tags").getItem(0).alias("payment_method"),
        lit("PAID").alias("payment_status"),
        lit("Online").alias("region"),
        current_timestamp().alias("_processed_at")
    ).withColumn("payment_method", col("payment_method").cast("string")) \
     .na.fill({"payment_method": "ONLINE"})
     
    # Transform to Silver Order Items
    exploded_items = deduped_shopify.select(
        col("id").alias("order_id"),
        explode("lineItems.edges").alias("item_edge")
    ).select(
        col("order_id"),
        col("item_edge.node").alias("item")
    )
    
    items_df = exploded_items.select(
        col("item.id").alias("order_item_id"),
        col("order_id"),
        col("item.sku").alias("sku"),
        col("item.title").alias("title"),
        col("item.quantity").cast("int").alias("quantity"),
        col("item.variant.price").cast("double").alias("unit_price"),
        (col("item.quantity").cast("double") * col("item.variant.price").cast("double")).alias("total_amount"),
        current_timestamp().alias("_processed_at")
    )
    
    return orders_df, items_df

def process_pos_to_silver(spark):
    try:
        pos_bronze = spark.read.format("iceberg").load("nessie.bronze.pos_transactions")
        if pos_bronze.count() == 0:
            return None, None
    except Exception as e:
        print(f"POS Bronze table not available: {e}")
        return None, None
        
    # Deduplicate POS by transaction_id
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingest_time").desc())
    deduped_pos = pos_bronze.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
    
    # Transform to Silver Orders
    orders_df = deduped_pos.select(
        col("transaction_id").alias("order_id"),
        col("transaction_id").alias("order_number"),
        lit("pos").alias("channel"),
        col("timestamp").cast("timestamp").alias("created_at"),
        col("customer_loyalty_id").alias("customer_id"),
        lit(None).cast("string").alias("customer_email"),
        lit("INR").alias("currency"),
        col("subtotal_amount"),
        col("tax_amount"),
        col("total_amount"),
        col("payment_method"),
        lit("PAID").alias("payment_status"),
        col("store_name").alias("region"),
        current_timestamp().alias("_processed_at")
    )
    
    # Transform to Silver Order Items
    exploded_items = deduped_pos.select(
        col("transaction_id").alias("order_id"),
        explode("items").alias("item")
    )
    
    items_df = exploded_items.select(
        # Generate item id by hashing transaction and sku
        sha2(concat(col("order_id"), lit("-"), col("item.sku")), 256).alias("order_item_id"),
        col("order_id"),
        col("item.sku").alias("sku"),
        col("item.title").alias("title"),
        col("item.quantity").cast("int").alias("quantity"),
        col("item.unit_price").alias("unit_price"),
        col("item.amount").alias("total_amount"),
        current_timestamp().alias("_processed_at")
    )
    
    return orders_df, items_df

def process_distributor_to_silver(spark):
    try:
        dist_bronze = spark.read.format("iceberg").load("nessie.bronze.distributor_sales")
        if dist_bronze.count() == 0:
            return None, None
    except Exception as e:
        print(f"Distributor Bronze table not available: {e}")
        return None, None
        
    # De-duplicate rows by TransactionID
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("TransactionID").orderBy(col("_ingest_time").desc())
    deduped_dist = dist_bronze.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
    
    # Aggregate to Orders Level (distributor data is itemized)
    # Subtotal is unit_price * qty * (1 - discount/100)
    orders_df = deduped_dist.groupBy("InvoiceNumber").agg(
        min("InvoiceDate").cast("timestamp").alias("created_at"),
        first("DistributorID").alias("customer_id"),
        first("Region").alias("region"),
        first("PaymentStatus").alias("payment_status"),
        sum("UnitPrice").alias("subtotal_sum_placeholder"),  # temporary, we compute properly below
        sum("TaxAmount").alias("tax_amount"),
        sum("TotalAmount").alias("total_amount")
    ).select(
        col("InvoiceNumber").alias("order_id"),
        col("InvoiceNumber").alias("order_number"),
        lit("distributor").alias("channel"),
        col("created_at"),
        col("customer_id"),
        lit(None).cast("string").alias("customer_email"),
        lit("INR").alias("currency"),
        # subtotal = total_amount - tax_amount
        (col("total_amount") - col("tax_amount")).alias("subtotal_amount"),
        col("tax_amount"),
        col("total_amount"),
        lit("BANK_TRANSFER").alias("payment_method"),
        col("payment_status"),
        col("region"),
        current_timestamp().alias("_processed_at")
    )
    
    # Transform to Silver Order Items
    items_df = deduped_dist.select(
        col("TransactionID").alias("order_item_id"),
        col("InvoiceNumber").alias("order_id"),
        col("SKU").alias("sku"),
        # We don't have product title in distributor reports, we will assign sku or merge later
        col("SKU").alias("title"),
        col("Quantity").cast("int").alias("quantity"),
        col("UnitPrice").alias("unit_price"),
        col("TotalAmount").alias("total_amount"),
        current_timestamp().alias("_processed_at")
    )
    
    return orders_df, items_df

def process_amazon_to_silver(spark):
    try:
        amazon_bronze = spark.read.format("iceberg").load("nessie.bronze.amazon_sales")
        if amazon_bronze.count() == 0:
            return None, None
    except Exception as e:
        print(f"Amazon Bronze table not available: {e}")
        return None, None
        
    # Deduplicate Amazon by AmazonOrderId
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("AmazonOrderId").orderBy(col("_ingest_time").desc())
    deduped_amazon = amazon_bronze.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
    
    # Transform to Silver Orders
    orders_df = deduped_amazon.select(
        col("AmazonOrderId").alias("order_id"),
        col("SellerOrderId").alias("order_number"),
        lit("amazon").alias("channel"),
        col("PurchaseDate").cast("timestamp").alias("created_at"),
        col("BuyerEmail").alias("customer_id"),
        col("BuyerEmail").alias("customer_email"),
        col("OrderTotal.CurrencyCode").alias("currency"),
        col("OrderTotal.Amount").cast("double").alias("total_amount"),
        col("PaymentMethod").alias("payment_method"),
        lit("PAID").alias("payment_status"),
        col("SalesChannel").alias("region"),
        current_timestamp().alias("_processed_at")
    )
    
    # Extract items
    exploded_items = deduped_amazon.select(
        col("AmazonOrderId").alias("order_id"),
        explode("OrderItems").alias("item")
    )
    
    items_df = exploded_items.select(
        sha2(concat(col("order_id"), lit("-"), col("item.SellerSKU")), 256).alias("order_item_id"),
        col("order_id"),
        col("item.SellerSKU").alias("sku"),
        col("item.Title").alias("title"),
        col("item.QuantityOrdered").cast("int").alias("quantity"),
        (col("item.ItemPrice.Amount").cast("double") / col("item.QuantityOrdered").cast("double")).alias("unit_price"),
        col("item.ItemPrice.Amount").cast("double").alias("total_amount"),
        col("item.ItemTax.Amount").cast("double").alias("tax_amount"),
        current_timestamp().alias("_processed_at")
    )
    
    # Aggregate tax and subtotal from items
    aggregated_items = items_df.groupBy("order_id").agg(
        sum("tax_amount").alias("calculated_tax"),
        sum("total_amount").alias("calculated_subtotal")
    )
    
    orders_df = orders_df.join(aggregated_items, "order_id", "left") \
        .select(
            "order_id",
            "order_number",
            "channel",
            "created_at",
            "customer_id",
            "customer_email",
            "currency",
            col("calculated_subtotal").alias("subtotal_amount"),
            col("calculated_tax").alias("tax_amount"),
            "total_amount",
            "payment_method",
            "payment_status",
            "region",
            "_processed_at"
        )
        
    items_df_final = items_df.drop("tax_amount")
    
    return orders_df, items_df_final

def merge_silver_tables(spark, orders_df, items_df):
    """Executes a MERGE INTO to upsert data into the Silver tables."""
    if orders_df is None or items_df is None:
        return
        
    orders_df.createOrReplaceTempView("new_orders")
    items_df.createOrReplaceTempView("new_items")
    
    # Upsert Orders
    spark.sql("""
    MERGE INTO nessie.silver.orders t
    USING new_orders s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET
        t.order_number = s.order_number,
        t.created_at = s.created_at,
        t.customer_id = s.customer_id,
        t.customer_email = s.customer_email,
        t.subtotal_amount = s.subtotal_amount,
        t.tax_amount = s.tax_amount,
        t.total_amount = s.total_amount,
        t.payment_method = s.payment_method,
        t.payment_status = s.payment_status,
        t.region = s.region,
        t._processed_at = s._processed_at
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Completed Orders Merge.")
    
    # Upsert Order Items
    spark.sql("""
    MERGE INTO nessie.silver.order_items t
    USING new_items s
    ON t.order_item_id = s.order_item_id
    WHEN MATCHED THEN UPDATE SET
        t.order_id = s.order_id,
        t.sku = s.sku,
        t.title = s.title,
        t.quantity = s.quantity,
        t.unit_price = s.unit_price,
        t.total_amount = s.total_amount,
        t._processed_at = s._processed_at
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Completed Order Items Merge.")
    
    # Update products reference
    # Extract unique SKUs and Titles, and register/merge
    products_extracted = items_df.select("sku", "title", "unit_price") \
        .groupBy("sku").agg(
            first("title").alias("title"),
            first("unit_price").alias("retail_price")
        ).select(
            col("sku"),
            col("title"),
            col("retail_price"),
            current_timestamp().alias("_processed_at")
        )
        
    products_extracted.createOrReplaceTempView("new_products")
    spark.sql("""
    MERGE INTO nessie.silver.products t
    USING new_products s
    ON t.sku = s.sku
    WHEN MATCHED THEN UPDATE SET
        t.title = s.title,
        t.retail_price = s.retail_price,
        t._processed_at = s._processed_at
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Completed Products Reference Merge.")

def main():
    spark = get_spark_session("Refine-Bronze-To-Silver")
    
    create_silver_schemas(spark)
    
    # 1. Shopify
    sh_orders, sh_items = process_shopify_to_silver(spark)
    if sh_orders:
        print("Merging Shopify records to Silver...")
        merge_silver_tables(spark, sh_orders, sh_items)
        
    # 2. POS
    pos_orders, pos_items = process_pos_to_silver(spark)
    if pos_orders:
        print("Merging POS records to Silver...")
        merge_silver_tables(spark, pos_orders, pos_items)
        
    # 3. Distributor
    dist_orders, dist_items = process_distributor_to_silver(spark)
    if dist_orders:
        print("Merging Distributor records to Silver...")
        merge_silver_tables(spark, dist_orders, dist_items)
        
    # 4. Amazon
    amz_orders, amz_items = process_amazon_to_silver(spark)
    if amz_orders:
        print("Merging Amazon records to Silver...")
        merge_silver_tables(spark, amz_orders, amz_items)
        
    spark.stop()

if __name__ == "__main__":
    main()

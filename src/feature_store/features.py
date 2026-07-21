from datetime import timedelta
from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource,
)
from feast.types import Float64, Int64, String
from feast.value_type import ValueType

# 1. Define Entities
customer = Entity(
    name="customer",
    value_type=ValueType.STRING,
    join_keys=["customer_id"],
    description="CartCo customer identifier",
)

product = Entity(
    name="product",
    value_type=ValueType.STRING,
    join_keys=["sku"],
    description="CartCo product SKU identifier",
)

# 2. Define Data Sources pointing to exported Gold Layer parquet files
customer_source = FileSource(
    path="d:/LPU/Internship 26/data/feature_store/gold_customer_360.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

product_source = FileSource(
    path="d:/LPU/Internship 26/data/feature_store/gold_inventory_turnover.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# 3. Define Feature Views
customer_features_view = FeatureView(
    name="customer_features",
    entities=[customer],
    ttl=timedelta(days=90),
    schema=[
        Field(name="customer_email", dtype=String),
        Field(name="total_orders", dtype=Int64),
        Field(name="total_spend", dtype=Float64),
        Field(name="preferred_channel", dtype=String),
    ],
    online=True,
    source=customer_source,
    tags={"team": "analytics"},
)

product_features_view = FeatureView(
    name="product_features",
    entities=[product],
    ttl=timedelta(days=90),
    schema=[
        Field(name="product_title", dtype=String),
        Field(name="units_sold", dtype=Int64),
        Field(name="total_sales_amount", dtype=Float64),
    ],
    online=True,
    source=product_source,
    tags={"team": "merchandising"},
)

import os
import sys
import pandas as pd
import random
from datetime import datetime, timedelta

# Try importing streamlit
try:
    import streamlit as st
except ImportError:
    print("Streamlit not found. Please install it using: pip install streamlit")
    st = None

# If streamlit is not installed, provide a mock module so the script doesn't fail compilation
if st is None:
    class MockSt:
        def __getattr__(self, name):
            def mock_func(*args, **kwargs):
                return None
            return mock_func
    st = MockSt()

def load_gold_data_live():
    """Attempts to load Gold data from Spark/Iceberg/MinIO."""
    try:
        # Check if environment is configured
        if not os.environ.get("SPARK_HOME") and not os.path.exists("/opt/airflow"):
            # Skip if we are running locally without setup
            return None
            
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        from jobs.spark_helper import get_spark_session
        
        spark = get_spark_session("Dashboard-Reader")
        
        # Read Gold tables
        df_rev = spark.read.format("iceberg").load("nessie.gold.daily_revenue_channel").toPandas()
        df_inv = spark.read.format("iceberg").load("nessie.gold.inventory_turnover").toPandas()
        df_cust = spark.read.format("iceberg").load("nessie.gold.customer_360").toPandas()
        
        spark.stop()
        return df_rev, df_inv, df_cust
    except Exception as e:
        print(f"Failed to read from Spark/Iceberg catalog: {e}. Using mock dashboard data.")
        return None

def generate_mock_gold_data():
    """Generates synthetic Gold mart data for offline rendering."""
    # Daily Revenue
    channels = ["shopify", "pos", "distributor"]
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(14)]
    
    rev_rows = []
    for d in dates:
        for c in channels:
            if c == "shopify":
                orders = random.randint(50, 150)
                rev = orders * random.uniform(800.0, 1200.0)
            elif c == "pos":
                orders = random.randint(100, 300)
                rev = orders * random.uniform(500.0, 700.0)
            else: # distributor
                orders = random.randint(2, 8)
                rev = orders * random.uniform(25000.0, 75000.0)
                
            tax = rev * 0.18
            rev_rows.append({
                "sales_date": d,
                "channel": c,
                "order_count": orders,
                "gross_revenue": round(rev, 2),
                "tax_collected": round(tax, 2),
                "average_order_value": round(rev / orders, 2)
            })
    df_rev = pd.DataFrame(rev_rows)
    
    # Inventory
    skus = [
        {"sku": "SKU-TSHIRT-01", "title": "CartCo Classic White T-Shirt"},
        {"sku": "SKU-JEANS-02", "title": "CartCo Slim Fit Blue Jeans"},
        {"sku": "SKU-HOODIE-03", "title": "CartCo Fleece Hoodie Black"},
        {"sku": "SKU-SOCKS-04", "title": "CartCo Ankle Socks (3-Pack)"},
        {"sku": "SKU-JACKET-05", "title": "CartCo Premium Denim Jacket"},
    ]
    inv_rows = []
    for s in skus:
        units = random.randint(500, 2000)
        inv_rows.append({
            "sku": s["sku"],
            "product_title": s["title"],
            "units_sold": units,
            "total_sales_amount": round(units * random.uniform(300.0, 1500.0), 2)
        })
    df_inv = pd.DataFrame(inv_rows)
    
    # Customer 360
    cust_rows = []
    domains = ["gmail.com", "yahoo.com", "outlook.com"]
    names = ["Aarav", "Dia", "Kabir", "Ira", "Reyansh", "Myra", "Vihaan", "Aanya", "Ishaan", "Sai"]
    for i in range(15):
        name = random.choice(names)
        cust_rows.append({
            "customer_id": f"LOY-{random.randint(10000, 99999)}",
            "customer_email": f"{name.lower()}{random.randint(10,99)}@{random.choice(domains)}",
            "first_purchase_date": (datetime.now() - timedelta(days=random.randint(30, 90))).strftime("%Y-%m-%d %H:%M:%S"),
            "last_purchase_date": (datetime.now() - timedelta(days=random.randint(1, 10))).strftime("%Y-%m-%d %H:%M:%S"),
            "total_orders": random.randint(1, 12),
            "total_spend": round(random.uniform(500.0, 15000.0), 2),
            "preferred_channel": random.choice(["shopify", "pos"])
        })
    df_cust = pd.DataFrame(cust_rows)
    
    return df_rev, df_inv, df_cust

def build_dashboard():
    st.set_page_config(
        page_title="CartCo Gold Data Marts",
        page_icon="📈",
        layout="wide"
    )
    
    st.title("🛒 CartCo Unified Commerce Lakehouse Dashboard")
    st.markdown("### Real-time Medallion Lakehouse Marts (Gold Layer)")
    st.markdown("---")
    
    # Load data
    data = load_gold_data_live()
    if data:
        st.success("Successfully loaded live data from Apache Iceberg + Project Nessie Catalog!")
        df_rev, df_inv, df_cust = data
    else:
        st.warning("Could not connect to Iceberg catalog. Showing simulated data from Gold mart definitions.")
        df_rev, df_inv, df_cust = generate_mock_gold_data()
        
    # Layout - High Level Metrics
    total_rev = df_rev["gross_revenue"].sum()
    total_orders = df_rev["order_count"].sum()
    total_tax = df_rev["tax_collected"].sum()
    
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Gross Revenue (Gold)", f"₹{total_rev:,.2f}")
    m2.metric("Total Orders Ingested", f"{total_orders:,}")
    m3.metric("Total Tax (GST 18%)", f"₹{total_tax:,.2f}")
    
    st.markdown("---")
    
    # Tabs for different marts
    tab1, tab2, tab3 = st.tabs(["📈 Daily & Channel Sales", "📦 Inventory & SKUs", "👤 Customer 360"])
    
    with tab1:
        st.subheader("Daily Sales Performance by Channel")
        
        # Pivot table for plotting
        rev_pivot = df_rev.pivot_table(
            index="sales_date", 
            columns="channel", 
            values="gross_revenue", 
            aggfunc="sum"
        ).fillna(0)
        
        st.bar_chart(rev_pivot)
        
        st.subheader("Raw Daily Sales Matrix")
        st.dataframe(df_rev.sort_values(by="sales_date", ascending=False), use_container_width=True)
        
    with tab2:
        st.subheader("SKU Sales & Turnover Analysis")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.dataframe(df_inv.sort_values(by="units_sold", ascending=False), use_container_width=True)
        with col2:
            st.write("Top Sold Products by Revenue")
            top_products = df_inv.sort_values(by="total_sales_amount", ascending=False)
            st.bar_chart(top_products.set_index("product_title")["total_sales_amount"])
            
    with tab3:
        st.subheader("Customer Profiles (360 Degree View)")
        st.dataframe(df_cust.sort_values(by="total_spend", ascending=False), use_container_width=True)

if __name__ == "__main__":
    if "streamlit" in sys.modules:
        build_dashboard()

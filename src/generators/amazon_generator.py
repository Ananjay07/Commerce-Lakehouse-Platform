import json
import os
import random
import uuid
from datetime import datetime, timedelta

# Create landing zone directory if it doesn't exist
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/amazon_drops"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

SKUS = [
    {"sku": "SKU-TSHIRT-01", "asin": "B08XM6H21A", "title": "CartCo Classic White T-Shirt", "price": 499.00},
    {"sku": "SKU-JEANS-02", "asin": "B08XM7Y52B", "title": "CartCo Slim Fit Blue Jeans", "price": 1299.00},
    {"sku": "SKU-HOODIE-03", "asin": "B08XM9Z83C", "title": "CartCo Fleece Hoodie Black", "price": 1899.00},
    {"sku": "SKU-SOCKS-04", "asin": "B08XM9W24D", "title": "CartCo Ankle Socks (3-Pack)", "price": 299.00},
    {"sku": "SKU-JACKET-05", "asin": "B08XM8X15E", "title": "CartCo Premium Denim Jacket", "price": 2499.00},
]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
FIRST_NAMES = ["Amit", "Neha", "Rahul", "Pooja", "Vikram", "Sneha", "Karan", "Priya", "Rohan", "Ananya"]
LAST_NAMES = ["Sharma", "Verma", "Gupta", "Patel", "Singh", "Joshi", "Mehta", "Reddy", "Nair", "Rao"]

def generate_amazon_order(order_num, timestamp):
    num_items = random.randint(1, 3)
    order_items = []
    subtotal = 0.0
    
    selected_skus = random.sample(SKUS, min(num_items, len(SKUS)))
    for s in selected_skus:
        qty = random.randint(1, 2)
        item_total = s["price"] * qty
        subtotal += item_total
        order_items.append({
            "ASIN": s["asin"],
            "SellerSKU": s["sku"],
            "Title": s["title"],
            "QuantityOrdered": qty,
            "QuantityShipped": qty,
            "ItemPrice": {
                "CurrencyCode": "INR",
                "Amount": f"{item_total:.2f}"
            },
            "ItemTax": {
                "CurrencyCode": "INR",
                "Amount": f"{item_total * 0.18:.2f}" # 18% tax
            }
        })
        
    tax = round(subtotal * 0.18, 2)
    total = subtotal + tax
    
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    buyer_name = f"{first_name} {last_name}"
    email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(DOMAINS)}"
    
    # Standard Amazon Order ID format: 3-7-7 digits
    amazon_order_id = f"{random.randint(100, 999)}-{random.randint(1000000, 9999999)}-{random.randint(1000000, 9999999)}"
    
    return {
        "AmazonOrderId": amazon_order_id,
        "SellerOrderId": f"AMZ-CC-{order_num}",
        "PurchaseDate": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "LastUpdateDate": (timestamp + timedelta(minutes=random.randint(10, 60))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "OrderStatus": "Shipped",
        "FulfillmentChannel": random.choice(["AFN", "MFN"]), # AFN = FBA, MFN = Seller Fulfilled
        "SalesChannel": "Amazon.in",
        "OrderTotal": {
            "CurrencyCode": "INR",
            "Amount": f"{total:.2f}"
        },
        "NumberOfItemsShipped": num_items,
        "NumberOfItemsUnshipped": 0,
        "PaymentMethod": random.choice(["COD", "Other"]),
        "BuyerEmail": email,
        "BuyerInfo": {
            "BuyerName": buyer_name,
            "BuyerCounty": "IN"
        },
        "OrderItems": order_items
    }

def generate_batch(batch_size=10):
    timestamp = datetime.utcnow()
    orders = []
    start_order_num = random.randint(30000, 40000)
    
    for i in range(batch_size):
        order_time = timestamp - timedelta(minutes=random.randint(1, 120))
        orders.append(generate_amazon_order(start_order_num + i, order_time))
        
    payload = {
        "Orders": orders,
        "CreatedTime": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "NextToken": str(uuid.uuid4())
    }
    
    filename = f"amazon_orders_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    with open(filepath, 'w') as f:
        json.dump(payload, f, indent=2)
        
    print(f"Generated {batch_size} Amazon SP-API orders and saved to: {filepath}")
    return filepath

if __name__ == "__main__":
    generate_batch(random.randint(5, 12))

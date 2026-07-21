import json
import os
import random
import uuid
from datetime import datetime, timedelta

# Create landing zone directory if it doesn't exist
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/shopify_drops"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

SKUS = [
    {"sku": "SKU-TSHIRT-01", "title": "CartCo Classic White T-Shirt", "price": 499.00},
    {"sku": "SKU-JEANS-02", "title": "CartCo Slim Fit Blue Jeans", "price": 1299.00},
    {"sku": "SKU-HOODIE-03", "title": "CartCo Fleece Hoodie Black", "price": 1899.00},
    {"sku": "SKU-SOCKS-04", "title": "CartCo Ankle Socks (3-Pack)", "price": 299.00},
    {"sku": "SKU-JACKET-05", "title": "CartCo Premium Denim Jacket", "price": 2499.00},
]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
FIRST_NAMES = ["Ananya", "Rahul", "Priya", "Amit", "Sneha", "Vikram", "Neha", "Rohan", "Karan", "Pooja"]
LAST_NAMES = ["Sharma", "Verma", "Gupta", "Patel", "Singh", "Joshi", "Mehta", "Reddy", "Nair", "Rao"]

def generate_shopify_order(order_num, timestamp):
    num_items = random.randint(1, 3)
    line_items = []
    subtotal = 0.0
    
    selected_skus = random.sample(SKUS, min(num_items, len(SKUS)))
    for s in selected_skus:
        qty = random.randint(1, 2)
        item_total = s["price"] * qty
        subtotal += item_total
        line_items.append({
            "id": f"gid://shopify/LineItem/{random.randint(100000000, 999999999)}",
            "title": s["title"],
            "quantity": qty,
            "sku": s["sku"],
            "variant": {
                "price": f"{s['price']:.2f}",
                "sku": s["sku"]
            }
        })
        
    tax = round(subtotal * 0.18, 2) # 18% GST
    total = subtotal + tax
    
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(DOMAINS)}"
    
    order_id = f"gid://shopify/Order/{random.randint(1000000000, 9999999999)}"
    
    return {
        "node": {
            "id": order_id,
            "name": f"#CC-{order_num}",
            "createdAt": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "totalPriceSet": {
                "presentmentMoney": {
                    "amount": f"{total:.2f}",
                    "currencyCode": "INR"
                }
            },
            "subtotalPriceSet": {
                "presentmentMoney": {
                    "amount": f"{subtotal:.2f}",
                    "currencyCode": "INR"
                }
            },
            "totalTaxSet": {
                "presentmentMoney": {
                    "amount": f"{tax:.2f}",
                    "currencyCode": "INR"
                }
            },
            "email": email,
            "customer": {
                "id": f"gid://shopify/Customer/{random.randint(1000000000, 9999999999)}",
                "firstName": first_name,
                "lastName": last_name,
                "phone": f"+91{random.randint(7000000000, 9999999999)}"
            },
            "lineItems": {
                "edges": [{"node": item} for item in line_items]
            },
            "sourceName": "web",
            "tags": random.choice([[], ["discount-applied"], ["first-order"], ["loyalty-member"]])
        }
    }

def generate_batch(batch_size=10):
    timestamp = datetime.utcnow()
    orders = []
    # Fetch latest order number prefix or generate random
    start_order_num = random.randint(10000, 20000)
    
    for i in range(batch_size):
        # Stagger orders slightly back in time
        order_time = timestamp - timedelta(minutes=random.randint(1, 120))
        orders.append(generate_shopify_order(start_order_num + i, order_time))
        
    # Wrap in Shopify-like response envelope
    payload = {
        "data": {
            "orders": {
                "edges": orders,
                "pageInfo": {
                    "hasNextPage": False,
                    "endCursor": str(uuid.uuid4())
                }
            }
        }
    }
    
    filename = f"shopify_orders_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    with open(filepath, 'w') as f:
        json.dump(payload, f, indent=2)
        
    print(f"Generated {batch_size} Shopify orders and saved to: {filepath}")
    return filepath

if __name__ == "__main__":
    generate_batch(random.randint(5, 15))

import json
import os
import random
import time
from datetime import datetime

# Local directory fallback if Kafka is not available
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/pos_drops"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

STORES = [
    {"id": "STORE-01", "name": "CartCo Mumbai Colaba"},
    {"id": "STORE-02", "name": "CartCo Delhi Connaught Place"},
    {"id": "STORE-03", "name": "CartCo Bengaluru Indiranagar"},
    {"id": "STORE-04", "name": "CartCo Chennai T-Nagar"},
    {"id": "STORE-05", "name": "CartCo Kolkata Park Street"},
    {"id": "STORE-06", "name": "CartCo Hyderabad Jubilee Hills"},
    {"id": "STORE-07", "name": "CartCo Pune Koregaon Park"},
    {"id": "STORE-08", "name": "CartCo Ahmedabad Satellite"},
]

SKUS = [
    {"sku": "SKU-TSHIRT-01", "title": "CartCo Classic White T-Shirt", "price": 499.00},
    {"sku": "SKU-JEANS-02", "title": "CartCo Slim Fit Blue Jeans", "price": 1299.00},
    {"sku": "SKU-HOODIE-03", "title": "CartCo Fleece Hoodie Black", "price": 1899.00},
    {"sku": "SKU-SOCKS-04", "title": "CartCo Ankle Socks (3-Pack)", "price": 299.00},
    {"sku": "SKU-JACKET-05", "title": "CartCo Premium Denim Jacket", "price": 2499.00},
]

PAYMENT_METHODS = ["UPI", "UPI", "CARD", "CARD", "CASH"]

def generate_pos_transaction():
    store = random.choice(STORES)
    num_items = random.randint(1, 4)
    items = []
    subtotal = 0.0
    
    selected_skus = random.sample(SKUS, min(num_items, len(SKUS)))
    for s in selected_skus:
        qty = random.randint(1, 3)
        amt = s["price"] * qty
        subtotal += amt
        items.append({
            "sku": s["sku"],
            "title": s["title"],
            "quantity": qty,
            "unit_price": s["price"],
            "amount": amt
        })
        
    tax = round(subtotal * 0.18, 2)
    total = round(subtotal + tax, 2)
    
    tx_id = f"TXN-POS-{store['id']}-{random.randint(10000000, 99999999)}"
    
    tx = {
        "transaction_id": tx_id,
        "store_id": store["id"],
        "store_name": store["name"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cashier_id": f"CASH-{random.randint(100, 999)}",
        "items": items,
        "subtotal_amount": subtotal,
        "tax_amount": tax,
        "total_amount": total,
        "payment_method": random.choice(PAYMENT_METHODS),
        "customer_loyalty_id": f"LOY-{random.randint(10000, 99999)}" if random.random() > 0.5 else None
    }
    return tx

def publish_transactions(num_tx=5, kafka_bootstrap_servers="localhost:9092"):
    producer = None
    
    # Try importing confluent_kafka to publish to broker
    try:
        from confluent_kafka import Producer
        producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'client.id': 'pos-generator',
            'socket.timeout.ms': 1000,
            'message.timeout.ms': 2000
        })
        # Test connection
        producer.list_topics(timeout=1.0)
        print(f"Connected to Kafka broker at {kafka_bootstrap_servers}")
    except Exception as e:
        print(f"Could not connect to Kafka broker ({e}). Falling back to local JSON drop mode.")
        producer = None
        
    transactions = []
    
    for i in range(num_tx):
        tx = generate_pos_transaction()
        transactions.append(tx)
        
        if producer:
            try:
                producer.produce(
                    'pos-transactions', 
                    key=tx['transaction_id'], 
                    value=json.dumps(tx).encode('utf-8')
                )
                print(f"Published POS TXN: {tx['transaction_id']} to Kafka.")
            except Exception as e:
                print(f"Failed to publish transaction {tx['transaction_id']}: {e}")
        
        # Stagger if multiple runs
        if i < num_tx - 1:
            time.sleep(0.1)
            
    if producer:
        producer.flush()
    else:
        # Write to local file drop
        timestamp_str = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"pos_transactions_{timestamp_str}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        with open(filepath, 'w') as f:
            json.dump(transactions, f, indent=2)
        print(f"Saved {num_tx} transactions locally to: {filepath}")

if __name__ == "__main__":
    import sys
    servers = "localhost:9092"
    if len(sys.argv) > 1:
        servers = sys.argv[1]
        
    publish_transactions(random.randint(5, 10), kafka_bootstrap_servers=servers)

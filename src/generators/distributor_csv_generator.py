import csv
import os
import random
from datetime import datetime, timedelta

# Create simulated SFTP directory if it doesn't exist
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/sftp_drops"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

DISTRIBUTORS = [
    {"id": "DIST-001", "name": "Apex Retail Distributors Ltd", "region": "North"},
    {"id": "DIST-002", "name": "Maratha Trade Connect", "region": "West"},
    {"id": "DIST-003", "name": "Dakshin Retail Logistics", "region": "South"},
    {"id": "DIST-004", "name": "Purvanchal Trade Linkers", "region": "East"},
]

SKUS = [
    {"sku": "SKU-TSHIRT-01", "price": 250.00},  # Distributor prices are lower (wholesale)
    {"sku": "SKU-JEANS-02", "price": 650.00},
    {"sku": "SKU-HOODIE-03", "price": 950.00},
    {"sku": "SKU-SOCKS-04", "price": 150.00},
    {"sku": "SKU-JACKET-05", "price": 1250.00},
]

def generate_csv_batch(num_rows=20):
    timestamp = datetime.utcnow()
    distributor = random.choice(DISTRIBUTORS)
    
    filename = f"distributor_sales_{distributor['id']}_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    headers = [
        "TransactionID", 
        "DistributorID", 
        "DistributorName", 
        "Region", 
        "InvoiceNumber", 
        "InvoiceDate", 
        "SKU", 
        "Quantity", 
        "UnitPrice", 
        "DiscountPercent",
        "TaxAmount", 
        "TotalAmount", 
        "PaymentStatus"
    ]
    
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        invoice_seq = random.randint(50000, 90000)
        
        for i in range(num_rows):
            tx_id = f"TXN-DIST-{random.randint(10000000, 99999999)}"
            invoice_num = f"INV-{invoice_seq + (i // 3)}" # 3 items per invoice on average
            invoice_date = (timestamp - timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d")
            
            sku_info = random.choice(SKUS)
            qty = random.choice([10, 20, 50, 100, 200]) # Bulk buys
            unit_price = sku_info["price"]
            
            discount = random.choice([0, 5, 10]) # 0%, 5%, 10%
            subtotal = (unit_price * qty) * (1 - discount / 100)
            tax = round(subtotal * 0.18, 2) # 18% GST
            total = round(subtotal + tax, 2)
            
            payment_status = random.choice(["PAID", "PAID", "PAID", "PENDING"])
            
            writer.writerow([
                tx_id,
                distributor["id"],
                distributor["name"],
                distributor["region"],
                invoice_num,
                invoice_date,
                sku_info["sku"],
                qty,
                f"{unit_price:.2f}",
                f"{discount}",
                f"{tax:.2f}",
                f"{total:.2f}",
                payment_status
            ])
            
    print(f"Generated {num_rows} distributor sale rows and saved to: {filepath}")
    return filepath

if __name__ == "__main__":
    generate_csv_batch(random.randint(15, 35))

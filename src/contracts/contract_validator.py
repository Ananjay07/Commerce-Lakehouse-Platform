import os
import re
import yaml

CONTRACT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "shopify_orders_contract.yaml"))

def load_contract():
    with open(CONTRACT_PATH, "r") as f:
        return yaml.safe_load(f)

def validate_record(order_node, contract):
    """
    Validates a single Shopify order node against the schema contract.
    Returns (is_valid, list_of_errors)
    """
    errors = []
    
    # Iterate through fields defined in contract
    for field in contract.get("fields", []):
        field_name = field["name"]
        is_required = field.get("required", False)
        expected_type = field.get("type", "string")
        
        # Check if field exists in the order node
        if field_name not in order_node:
            if is_required:
                errors.append(f"Missing required field: '{field_name}'")
            continue
            
        value = order_node[field_name]
        
        # Validate nullability
        if value is None:
            if is_required:
                errors.append(f"Required field '{field_name}' cannot be null")
            continue
            
        # Validate data types
        if expected_type == "string":
            if not isinstance(value, str):
                errors.append(f"Field '{field_name}' must be a string, got {type(value).__name__}")
            # Pattern validation (e.g. regex for email)
            elif "pattern" in field:
                pattern = field["pattern"]
                if not re.match(pattern, value):
                    errors.append(f"Field '{field_name}' value '{value}' does not match pattern '{pattern}'")
                    
        elif expected_type == "object":
            if not isinstance(value, dict):
                errors.append(f"Field '{field_name}' must be an object, got {type(value).__name__}")
                
        elif expected_type == "number":
            if not isinstance(value, (int, float)):
                errors.append(f"Field '{field_name}' must be a number, got {type(value).__name__}")
                
        elif expected_type == "array":
            if not isinstance(value, list):
                errors.append(f"Field '{field_name}' must be an array, got {type(value).__name__}")

    return len(errors) == 0, errors

def validate_shopify_file(file_path):
    """
    Parses a Shopify order file and validates all order nodes inside.
    Raises ValueError if critical contract checks fail.
    """
    import json
    
    print(f"Data Contract Verification: Validating file '{os.path.basename(file_path)}'...")
    contract = load_contract()
    
    with open(file_path, "r") as f:
        try:
            data = json.load(f)
        except Exception as e:
            raise ValueError(f"Invalid JSON format in file: {e}")
            
    # Drill down to edges
    try:
        edges = data["data"]["orders"]["edges"]
    except KeyError:
        raise ValueError("Invalid Shopify envelope: Missing data.orders.edges structure")
        
    all_valid = True
    total_errors = 0
    
    for i, edge in enumerate(edges):
        order_node = edge.get("node", {})
        order_name = order_node.get("name", f"Index {i}")
        
        is_valid, errors = validate_record(order_node, contract)
        if not is_valid:
            all_valid = False
            total_errors += len(errors)
            print(f"  [FAIL] Order {order_name} failed contract validation:")
            for err in errors:
                print(f"    - {err}")
                
    if not all_valid:
        raise ValueError(f"Data contract validation failed with {total_errors} errors in file '{os.path.basename(file_path)}'!")
        
    print(f"  [PASS] Data contract validation passed successfully for '{os.path.basename(file_path)}'.")
    return True

if __name__ == "__main__":
    # Test script with latest file in shopify_drops
    import glob
    drops_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/shopify_drops"))
    json_files = glob.glob(os.path.join(drops_dir, "*.json"))
    if json_files:
        latest_file = max(json_files, key=os.path.getctime)
        try:
            validate_shopify_file(latest_file)
        except Exception as e:
            print(f"Validation failed: {e}")
    else:
        print("No shopify drops found to test contract.")

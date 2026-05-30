import os
from pyiceberg.catalog import load_catalog
import pyarrow as pa

# 1. Configure Environment Variables for MinIO S3 Access
# PyIceberg uses these behind the scenes to authenticate with your storage layer
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password123"
os.environ["AWS_REGION"] = "us-east-1"

print("Connecting to Tabular REST Catalog on Kubernetes...")

# 2. Connect to the Catalog
catalog = load_catalog(
    "k8s-local",
    **{
        "type": "rest",
        "uri": "http://localhost:30181",                    # Catalog NodePort
        "s3.endpoint": "http://localhost:30090",            # MinIO NodePort
        "s3.path-style-access": "true",                     # Required for MinIO
    }
)

# 3. Create a Namespace (Database)
namespace = "dev_studio"
if namespace not in catalog.list_namespaces():
    catalog.create_namespace(namespace)
    print(f"-> Created namespace: '{namespace}'")

# 4. Define Table Schema and Identifier
table_expr = f"{namespace}.inventory"
schema = pa.schema([
    ("item_id", pa.int64()),
    ("item_name", pa.string()),
    ("quantity", pa.int64()),
    ("price", pa.float64())
])

# Drop table if it exists from a previous run to start perfectly fresh
if (namespace, "inventory") in catalog.list_tables(namespace):
    catalog.drop_table(table_expr)

# Create the fresh Iceberg table
table = catalog.create_table(table_expr, schema=schema)
print(f"-> Created Iceberg table: '{table_expr}'")


print("\n--- 1. INITIAL WRITE ---")
# Create initial dataset using PyArrow
initial_data = pa.Table.from_pydict({
    "item_id": [101, 102],
    "item_name": ["Laptop", "Mouse"],
    "quantity": [45, 120],
    "price": [999.99, 25.50]
})

# Commit write transaction to the catalog
table.append(initial_data)
print("Initial records committed successfully.")


print("\n--- 2. READ DATA ---")
# Reload the table to catch the latest snapshot state
table = catalog.load_table(table_expr)

# Scan all records and convert them into an Arrow table for printing
arrow_df = table.scan().to_arrow()
print(arrow_df.to_pylist())


print("\n--- 3. UPDATE / APPEND NEW DATA ---")
# Iceberg treats updates/appends as new immutable files logged to a new snapshot
new_data = pa.Table.from_pydict({
    "item_id": [103, 101],
    "item_name": ["Keyboard", "Laptop"],  # Appending an updated batch for item 101
    "quantity": [30, 5],                  # New incoming transaction quantities
    "price": [75.00, 999.99]
})

table.append(new_data)
print("New transaction logs appended to the table.")


print("\n--- 4. FINAL VERIFICATION READ ---")
table = catalog.load_table(table_expr)
final_df = table.scan().to_arrow()

# Print out final state
for row in final_df.to_pylist():
    print(f"ID: {row['item_id']} | Name: {row['item_name']} | Qty: {row['quantity']} | Price: ${row['price']:.2f}")
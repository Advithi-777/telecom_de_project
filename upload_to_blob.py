"""
Azure Blob Storage — Upload Raw Data
======================================
Uploads all files from ./raw_data/ into the 'raw' container
on Azure Blob Storage, partitioned by source and date.

Folder structure created in Blob:
  raw/
  ├── customers/dt=2024-01-01/customers.csv
  ├── cdr/dt=2024-01-01/cdr.csv
  ├── billing/dt=2024-01-01/billing.json
  └── network_kpi/dt=2024-01-01/network_kpi.csv

Prerequisites:
    pip install azure-storage-blob python-dotenv

Setup:
    Create a .env file in the project root:
        AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
        AZURE_CONTAINER_NAME=raw
"""

import os
from datetime import date
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME    = os.getenv("AZURE_CONTAINER_NAME", "raw")
LOCAL_DIR         = "./raw_data"
LOAD_DATE         = date.today().strftime("%Y-%m-%d")   # partition key

FILES = {
    "customers.csv":    "customers",
    "cdr.csv":          "cdr",
    "billing.json":     "billing",
    "network_kpi.csv":  "network_kpi",
}


def upload_files():
    if not CONNECTION_STRING:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set in .env")

    client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = client.get_container_client(CONTAINER_NAME)

    # Create container if it doesn't exist
    try:
        container_client.create_container()
        print(f"Created container: {CONTAINER_NAME}")
    except Exception:
        print(f"Container '{CONTAINER_NAME}' already exists — skipping creation")

    for filename, folder in FILES.items():
        local_path = os.path.join(LOCAL_DIR, filename)
        blob_path  = f"{folder}/dt={LOAD_DATE}/{filename}"

        if not os.path.exists(local_path):
            print(f"  SKIP  {filename} — file not found at {local_path}")
            continue

        with open(local_path, "rb") as data:
            container_client.upload_blob(
                name=blob_path,
                data=data,
                overwrite=True
            )
        file_size = os.path.getsize(local_path) / 1024
        print(f"  OK    {blob_path}  ({file_size:.1f} KB)")

    print(f"\n✓ Upload complete — partition date: dt={LOAD_DATE}")
    print(f"  Storage path: {CONTAINER_NAME}/{{source}}/dt={LOAD_DATE}/")


if __name__ == "__main__":
    upload_files()

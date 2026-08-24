import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


# --------------------------------------------------
# Config
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("TRAFIKLAB_REALTIME_API_KEY")

if not API_KEY:
    raise ValueError("TRAFIKLAB_REALTIME_API_KEY is missing from .env")


STORAGE_ACCOUNT_NAME = "ststockholmtransitdev"
FILE_SYSTEM_NAME = "raw"

GTFS_RT_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-rt-sweden/sl/TripUpdatesSweden.pb"
)


# --------------------------------------------------
# 1. Download GTFS-Realtime TripUpdates
# --------------------------------------------------

print("Downloading SL GTFS-Realtime TripUpdates...")

response = requests.get(
    GTFS_RT_URL,
    params={"key": API_KEY},
    timeout=30,
)

response.raise_for_status()

data = response.content

print(f"Downloaded {len(data) / 1024:.2f} KB")

# --------------------------------------------------
# Save local working copy
# --------------------------------------------------

from pathlib import Path

LOCAL_REALTIME_FILE = Path(
    "data/raw/gtfs_realtime/TripUpdatesSweden.pb"
)

LOCAL_REALTIME_FILE.parent.mkdir(
    parents=True,
    exist_ok=True,
)

LOCAL_REALTIME_FILE.write_bytes(data)

print(
    f"Saved local working copy: "
    f"{LOCAL_REALTIME_FILE}"
)

# --------------------------------------------------
# 2. Connect to ADLS Gen2
# --------------------------------------------------

account_url = (
    f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
)

credential = DefaultAzureCredential()

service_client = DataLakeServiceClient(
    account_url=account_url,
    credential=credential,
)

file_system_client = service_client.get_file_system_client(
    file_system=FILE_SYSTEM_NAME
)


# --------------------------------------------------
# 3. Build timestamped raw directory hierarchy
# --------------------------------------------------

ingestion_time = datetime.now(timezone.utc)

directory_parts = [
    "trafiklab",
    "gtfs_realtime",
    "trip_updates",
    f"ingestion_date={ingestion_time:%Y-%m-%d}",
    f"hour={ingestion_time:%H}",
]

current_path = ""

for part in directory_parts:
    current_path = (
        f"{current_path}/{part}"
        if current_path
        else part
    )

    directory_client = file_system_client.get_directory_client(
        current_path
    )

    try:
        directory_client.create_directory()
        print(f"Created directory: raw/{current_path}")
    except ResourceExistsError:
        pass


# --------------------------------------------------
# 4. Upload raw protobuf snapshot
# --------------------------------------------------

file_name = f"trip_updates_{ingestion_time:%H%M%S}.pb"

file_client = directory_client.get_file_client(file_name)

print(f"Uploading to raw/{current_path}/{file_name}...")

file_client.upload_data(
    data,
    overwrite=True,
)


# --------------------------------------------------
# Done
# --------------------------------------------------

print("\nSUCCESS")
print(f"Uploaded: raw/{current_path}/{file_name}")
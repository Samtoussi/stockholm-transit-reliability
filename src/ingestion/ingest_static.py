import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


STORAGE_ACCOUNT_NAME = "ststockholmtransitdev"
FILE_SYSTEM_NAME = "raw"

TRAFIKLAB_STATIC_URL = (
    "https://opendata.samtrafiken.se/gtfs-sweden/sweden.zip"
)


load_dotenv()

api_key = os.getenv("TRAFIKLAB_STATIC_API_KEY")

if not api_key:
    raise ValueError("TRAFIKLAB_STATIC_API_KEY is missing from .env")


# --------------------------------------------------
# 1. Download GTFS Static to temporary local file
# --------------------------------------------------

print("Downloading GTFS Sweden 3 Static...")

with requests.get(
    TRAFIKLAB_STATIC_URL,
    params={"key": api_key},
    stream=True,
    timeout=120,
) as response:
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(
        suffix=".zip",
        delete=False,
    ) as temp_file:

        temp_path = Path(temp_file.name)

        downloaded_bytes = 0

        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                temp_file.write(chunk)
                downloaded_bytes += len(chunk)


print(f"Downloaded {downloaded_bytes / 1024 / 1024:.2f} MB")


# --------------------------------------------------
# 2. Connect to ADLS
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
# 3. Build Bronze/raw directory hierarchy
# --------------------------------------------------

ingestion_time = datetime.now(timezone.utc)

directory_parts = [
    "trafiklab",
    "gtfs_static",
    f"ingestion_date={ingestion_time:%Y-%m-%d}",
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
# 4. Upload original ZIP
# --------------------------------------------------

file_name = f"sweden_{ingestion_time:%H%M%S}.zip"

file_client = directory_client.get_file_client(file_name)

print(f"Uploading to raw/{current_path}/{file_name}...")


with open(temp_path, "rb") as data:
    file_client.upload_data(
        data,
        overwrite=True,
    )


# --------------------------------------------------
# 5. Clean up temporary file
# --------------------------------------------------

temp_path.unlink()


print("\nSUCCESS")
print(f"Uploaded: raw/{current_path}/{file_name}")
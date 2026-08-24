from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


STORAGE_ACCOUNT_NAME = "ststockholmtransitdev"
FILE_SYSTEM_NAME = "raw"


account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

credential = DefaultAzureCredential()

service_client = DataLakeServiceClient(
    account_url=account_url,
    credential=credential,
)

file_system_client = service_client.get_file_system_client(
    file_system=FILE_SYSTEM_NAME
)

directory_client = file_system_client.get_directory_client("test")
directory_client.create_directory()

file_client = directory_client.get_file_client("hello.txt")

data = b"Hello from Stockholm Transit Reliability!"

file_client.upload_data(
    data,
    overwrite=True,
)

print("SUCCESS")
print("Uploaded: raw/test/hello.txt")
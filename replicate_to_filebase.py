import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os

from dotenv import load_dotenv
load_dotenv()

# MinIO details
# Source: MinIO (Railway)
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET", "source-bucket")

# Target: Filebase
filebase_endpoint = os.getenv("FILEBASE_ENDPOINT")
filebase_access_key = os.getenv("FILEBASE_ACCESS_KEY")
filebase_secret_key = os.getenv("FILEBASE_SECRET_KEY")
filebase_bucket = os.getenv("FILEBASE_BUCKET", "replicate-bucket")

# Create S3 clients
# S3 config
minio_s3 = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

filebase_s3 = boto3.client(
    "s3",
    endpoint_url=filebase_endpoint,
    aws_access_key_id=filebase_access_key,
    aws_secret_access_key=filebase_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# ✅ Check if target bucket exists in Filebase, if not create it
try:
    filebase_s3.head_bucket(Bucket=filebase_bucket)
    print(f"✅ Filebase bucket '{filebase_bucket}' already exists.")
except ClientError as e:
    if e.response['Error']['Code'] == '404':
        filebase_s3.create_bucket(Bucket=filebase_bucket)
        print(f"✅ Filebase bucket '{filebase_bucket}' created.")
    else:
        raise

# ✅ List source objects from MinIO
source_objects = minio_s3.list_objects_v2(Bucket=minio_bucket_name).get("Contents", [])
print(f"📦 Found {len(source_objects)} object(s) in MinIO bucket '{minio_bucket_name}'")

# ✅ List existing objects in Filebase to skip duplicates
existing_objects = filebase_s3.list_objects_v2(Bucket=filebase_bucket).get("Contents", [])
existing_keys = set(obj["Key"] for obj in existing_objects) if existing_objects else set()

# 🔁 Replication
for obj in source_objects:
    key = obj["Key"]
    if key in existing_keys:
        print(f"⚠️ Skipping '{key}' (already exists in Filebase)")
        continue

    print(f"⬇️  Downloading '{key}' from MinIO...")
    response = minio_s3.get_object(Bucket=minio_bucket_name, Key=key)
    file_data = response["Body"].read()

    print(f"⬆️  Uploading '{key}' to Filebase...")
    filebase_s3.put_object(Bucket=filebase_bucket, Key=key, Body=file_data)
    print(f"✅ Replicated '{key}'")

print("🎉 Replication completed.")

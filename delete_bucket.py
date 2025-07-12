import boto3
from botocore.client import Config
import os

from dotenv import load_dotenv
load_dotenv()

# MinIO details
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET", "source-bucket")

# S3 config
s3 = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

try:
    # List and delete all objects in the bucket
    print(f"🧹 Deleting objects in bucket '{minio_bucket_name}'...")
    objects = s3.list_objects_v2(Bucket=minio_bucket_name)
    if "Contents" in objects:
        for obj in objects["Contents"]:
            s3.delete_object(Bucket=minio_bucket_name, Key=obj["Key"])
            print(f"   ❌ Deleted: {obj['Key']}")
    else:
        print("   ✅ Bucket is already empty.")

    # Now delete the bucket
    s3.delete_bucket(Bucket=minio_bucket_name)
    print(f"✅ Bucket '{minio_bucket_name}' deleted.")
except Exception as e:
    print(f"❌ Error deleting bucket: {e}")

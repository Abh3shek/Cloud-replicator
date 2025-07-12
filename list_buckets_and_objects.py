import boto3
from botocore.client import Config
import os

from dotenv import load_dotenv
load_dotenv()

# MinIO details
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")

# S3 config
s3 = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

response = s3.list_buckets()
print("🔍 Found", len(response['Buckets']), "bucket(s):\n")

for bucket in response['Buckets']:
    name = bucket['Name']
    print(f"📦 Bucket: {name}")
    try:
        objects = s3.list_objects_v2(Bucket=name)
        count = objects.get('KeyCount', 0)
        print(f"└─📄 Number of objects: {count}")

        if "Contents" in objects:
            for obj in objects["Contents"]:
                print(f"  └─ 📁 {obj['Key']}")
        else:
            print("     (No objects found)")
    except Exception as e:
        print(f"   ⚠️ Error listing objects: {e}")

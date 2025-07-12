import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from io import BytesIO
import os

from dotenv import load_dotenv
load_dotenv()

# MinIO details
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET", "source-bucket")

# Create S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# List Buckets
buckets = s3.list_buckets()["Buckets"]
print(f"🔍 Found {len(buckets)} bucket(s):\n")

for bucket in buckets:
    bucket_name = bucket["Name"]
    print(f"📦 Bucket: {bucket_name}")

    # ✅ Count objects
    objects = s3.list_objects_v2(Bucket=bucket_name)
    count = objects.get("KeyCount", 0)
    print(f"  └─📄 Number of objects: {count}")

# Create the bucket if it doesn't exist
try:
    s3.head_bucket(Bucket=minio_bucket_name)
    print(f"✅ Bucket '{minio_bucket_name}' already exists.")
except ClientError as e:
    if e.response['Error']['Code'] == '404':
        s3.create_bucket(Bucket=minio_bucket_name)
        print(f"✅ Bucket '{minio_bucket_name}' created.")
    else:
        print(f"❌ Error checking bucket: {e}")
        exit(1)

# Create a test file
file_name = "bye.txt"
file_content = "Bye abhishek from Minio on Railway!"
memory_file = BytesIO(file_content.encode("utf-8"))

# Upload the file to MinIO
try:
    s3.put_object(
        Bucket=minio_bucket_name,
        Key=file_name,
        Body=memory_file
    )
    print(f"✅ File '{file_name}' uploaded(or overwritten) to bucket '{minio_bucket_name} from memory'.")
except ClientError as e:
    print(f"❌ Failed to upload '{file_name}': {e}")
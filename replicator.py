from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os

# ✅ Load environment variables
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

# 📦 MinIO (source) config from env
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "source-bucket")

# ☁️ Filebase (target) config from env
FILEBASE_ENDPOINT = os.getenv("FILEBASE_ENDPOINT", "https://s3.filebase.com")
FILEBASE_ACCESS_KEY = os.getenv("FILEBASE_ACCESS_KEY")
FILEBASE_SECRET_KEY = os.getenv("FILEBASE_SECRET_KEY")
FILEBASE_BUCKET = os.getenv("FILEBASE_BUCKET", "replicated-bucket")

# 🚀 S3 clients
minio_s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

filebase_s3 = boto3.client(
    "s3",
    endpoint_url=FILEBASE_ENDPOINT,
    aws_access_key_id=FILEBASE_ACCESS_KEY,
    aws_secret_access_key=FILEBASE_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# 🧾 JSON payload schema
class ReplicationRequest(BaseModel):
    s3_key: str

@app.post("/v1/replicate")
def replicate_object(request: ReplicationRequest):
    key = request.s3_key

    # ✅ Check if object exists in source
    try:
        print(f"⬇️  Fetching '{key}' from MinIO...")
        response = minio_s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        stream = response["Body"].read()
    except ClientError as e:
        raise HTTPException(status_code=404, detail=f"Object '{key}' not found in MinIO")

    # ✅ Check if already exists in Filebase
    try:
        filebase_s3.head_object(Bucket=FILEBASE_BUCKET, Key=key)
        print(f"⚠️  Object '{key}' already exists in Filebase. Skipping upload.")
        return {"message": f"Object '{key}' already exists in Filebase (idempotent)."}
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise HTTPException(status_code=500, detail="Error checking Filebase")

    # ✅ Upload to Filebase
    try:
        print(f"⬆️  Uploading '{key}' to Filebase...")
        filebase_s3.put_object(Bucket=FILEBASE_BUCKET, Key=key, Body=stream)
        print(f"✅ Replicated '{key}'")
        return {"message": f"Replicated '{key}' successfully"}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

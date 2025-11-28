import os
import json
from minio import Minio
from minio.error import S3Error
from urllib.parse import quote

# Configuration
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET = 'artifacts'
PUBLIC_MINIO_ENDPOINT = os.environ.get("PUBLIC_MINIO_ENDPOINT", "localhost:9000")
PUBLIC_MINIO_SCHEME = os.environ.get("PUBLIC_MINIO_SCHEME", "https")

# Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def set_public_read_policy():
    """Set the bucket policy to allow public read (download) access."""
    try:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{MINIO_BUCKET}/*"}
            ]
        }
        minio_client.set_bucket_policy(MINIO_BUCKET, json.dumps(policy))
        print(f"Public read policy applied to bucket: {MINIO_BUCKET}")
    except S3Error as e:
        print(f"Error setting bucket policy: {e}")

def init_minio_bucket():
    """Initialize MinIO bucket if it doesn't exist."""
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            print(f"Created bucket: {MINIO_BUCKET}")
        set_public_read_policy()
    except S3Error as e:
        print(f"Error checking/creating bucket: {e}")

def build_public_url(bucket_name: str, object_name: str) -> str:
    """Build a public HTTP URL for an object in MinIO."""
    encoded_key = quote(object_name, safe='/')
    return f"{PUBLIC_MINIO_SCHEME}://{PUBLIC_MINIO_ENDPOINT}/{bucket_name}/{encoded_key}"
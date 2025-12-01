import os
import json
import psycopg2
from functools import wraps
from flask import request
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from minio import Minio
from minio.error import S3Error
from urllib.parse import quote

# --- Configuration ---
# MinIO
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET = 'artifacts'
PUBLIC_MINIO_ENDPOINT = os.environ.get("PUBLIC_MINIO_ENDPOINT", "localhost:9000")
PUBLIC_MINIO_SCHEME = os.environ.get("PUBLIC_MINIO_SCHEME", "https")

# Kafka
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')

# --- This is where new topics are added ---
ARTIFACTS_TOPIC = 'artifacts'
SENSOR_READINGS_TOPIC = 'sensor_readings'
ARTIFACT_UPLOADED_TOPIC = 'artifact_uploaded'
SENSOR_READING_UPLOADED_TOPIC = 'sensor_reading_uploaded'

# Postgres
PG_HOST = os.environ.get('PG_HOST', 'postgres')
PG_PORT = os.environ.get('PG_PORT', '5432')
PG_DB = os.environ.get('PG_DB')
PG_USER = os.environ.get('PG_USER')
PG_PASSWORD = os.environ.get('PG_PASSWORD')

# Auth
MASTER_API_KEY = os.environ.get('API_SECRET_KEY')

# --- Clients ---
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Schema registry
simple_producer = Producer({'bootstrap.servers': KAFKA_BROKER})
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# --- Helper Functions ---

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

# Run init immediately when this module is imported
init_minio_bucket()

def build_public_url(bucket_name: str, object_name: str) -> str:
    """Build a public HTTP URL for an object in MinIO."""
    encoded_key = quote(object_name, safe='/')
    return f"{PUBLIC_MINIO_SCHEME}://{PUBLIC_MINIO_ENDPOINT}/{bucket_name}/{encoded_key}"

def get_db_connection():
    """Creates and returns a new connection to the Postgres database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD
    )

def require_api_key(f):
    """Decorator to require API Key authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        provided_key = request.headers.get('Authorization')
        if MASTER_API_KEY and provided_key == f"Bearer {MASTER_API_KEY}":
            return f(*args, **kwargs)
        return {'error': 'Unauthorized'}, 401
    return decorated_function

def send_avro_message(topic, key, value, schema_str):
    """Serializes a message using Avro and sends it to Kafka."""
    try:
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        serialized_value = avro_serializer(value, SerializationContext(topic, MessageField.VALUE))
        simple_producer.produce(
            topic=topic,
            key=str(key).encode('utf-8'),
            value=serialized_value
        )
        simple_producer.flush()
        print(f"Sent to Kafka topic {topic}: {key}")
        return True
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        return False

def send_to_kafka_simple(topic, key, value):
    """Sends a simple JSON message without schema registry structure."""
    try:
        simple_producer.produce(
            topic=topic,
            key=str(key).encode('utf-8'),
            value=json.dumps(value).encode('utf-8')
        )
        simple_producer.flush()
        print(f"Sent to Kafka topic {topic}: {key}")
        return True
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        return False
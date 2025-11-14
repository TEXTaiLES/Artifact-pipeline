from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_restful import Api, Resource
from confluent_kafka import Producer
import json
import uuid
from datetime import datetime, timezone
from minio import Minio
from minio.error import S3Error
import io
from flasgger import Swagger
import psycopg2
import os
from functools import wraps
from urllib.parse import quote

app = Flask(__name__)

# Serve the static OpenAPI spec at /swagger.json
@app.route('/swagger.json')
def swagger_spec():
    return send_from_directory('static', 'swagger.json')

Swagger(app, config={
    'specs': [
        {
            'endpoint': 'swagger',
            'route': '/swagger.json',
            'rule_filter': lambda rule: True,  # all endpoints
            'model_filter': lambda tag: True,  # all models
        }
    ],
    'static_url_path': '/flasgger_static',
    'swagger_ui': True,
    'specs_route': '/docs',
    'headers': []
})
api = Api(app)

# MinIO configuration for object storage
MINIO_ENDPOINT = 'minio:9000'  # Use Docker service name
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'artifacts'

# Public MinIO URL configuration (what clients use in URLs)
PUBLIC_MINIO_ENDPOINT = os.getenv("PUBLIC_MINIO_ENDPOINT", "localhost:9000")
PUBLIC_MINIO_SCHEME = os.getenv("PUBLIC_MINIO_SCHEME", "https")

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure the MinIO bucket exists
def init_minio_bucket():
    """Initialize MinIO bucket if it doesn't exist."""
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            print(f"Created bucket: {MINIO_BUCKET}")

        set_public_read_policy()
    except S3Error as e:
        print(f"Error creating bucket: {e}")

# Set public read access policy on the bucket
def set_public_read_policy():
    """Set the bucket policy to allow public read (download) access."""
    try:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},  # Allow any principal (public access)
                    "Action": "s3:GetObject",  # Only allow read access
                    "Resource": f"arn:aws:s3:::{MINIO_BUCKET}/*"
                }
            ]
        }
        # Convert the policy to a JSON string
        policy_json = json.dumps(policy)

        # Apply the policy
        minio_client.set_bucket_policy(MINIO_BUCKET, policy_json)
        print(f"Public read policy applied to bucket: {MINIO_BUCKET}")
    except S3Error as e:
        print(f"Error setting bucket policy: {e}")

init_minio_bucket()

def build_public_url(bucket_name: str, object_name: str) -> str:
    """Build a public HTTP URL for an object in MinIO."""
    # Encode path safely for URL
    encoded_key = quote(object_name, safe='/')
    return f"{PUBLIC_MINIO_SCHEME}://{PUBLIC_MINIO_ENDPOINT}/{bucket_name}/{encoded_key}"

# Postgres configuration for accessing the database
PG_HOST = 'postgres'
PG_PORT = '5432'
PG_DB = 'mydb'
PG_USER = 'admin'
PG_PASSWORD = 'admin123'

# Authentication requirement
MASTER_API_KEY = os.environ.get('API_SECRET_KEY')

def require_api_key(f):
    """A decorator to check for a valid API key in the request header."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if the key was provided in the 'Authorization' header
        provided_key = request.headers.get('Authorization')

        # Check if the key is valid (and that we have a master key set up)
        if MASTER_API_KEY and provided_key == f"Bearer {MASTER_API_KEY}":
            # Key is valid, proceed with the original function
            return f(*args, **kwargs)
        else:
            # Key is missing or incorrect
            return {'error': 'Unauthorized'}, 401
    return decorated_function

# Kafka configuration for event streaming
KAFKA_BROKER = 'kafka:29092'  # Use Docker service name
ARTIFACTS_TOPIC = 'artifacts'
ARTIFACT_UPLOADED_TOPIC = 'artifact_uploaded'

# Initialize Kafka producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def send_to_kafka_with_schema(topic, key, value):
    """Send structured message with JSON schema to Kafka topic."""
    try:
        structured_message = {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": False, "field": "artifact_id"},
                    {"type": "string", "optional": True, "field": "title"},
                    {"type": "string", "optional": False, "field": "filename"},
                    {"type": "string", "optional": False, "field": "location"},
                    {"type": "string", "optional": True, "field": "public_url"},

                    # New dummy datatype added
                    {"type": "string", "optional": True, "field": "drone_id"},

                    {"type": "string", "optional": True, "field": "uploaded_by"},
                    {
                        "type": "int64",
                        "optional": True,
                        "name": "org.apache.kafka.connect.data.Timestamp",
                        "version": 1,
                        "field": "timestamp"
                    }
                ],
                "optional": False,
                "name": "artifact"
            },
            "payload": value
        }

        producer.produce(
            topic=topic,
            key=str(key).encode('utf-8'),
            value=json.dumps(structured_message).encode('utf-8')
        )
        producer.flush()
        print(f"Sent to Kafka topic {topic}: {key}")
        return True
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        return False

def send_to_kafka_simple(topic, key, value):
    """Send simple JSON message without schema to Kafka topic."""
    try:
        producer.produce(
            topic=topic,
            key=str(key).encode('utf-8'),
            value=json.dumps(value).encode('utf-8')
        )
        producer.flush()
        print(f"Sent to Kafka topic {topic}: {key}")
        return True
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        return False

class ArtifactResource(Resource):
    # Authentication
    method_decorators = [require_api_key]

    def post(self):
        """Enhanced post method, allowing single-file
        or batch uploads using JSON metadata mapping"""

        files = request.files.getlist('file')
        if not files or files[0].filename == '':
            return {'error': 'No file(s) provided'}, 400

        uploaded_artifacts = []

        # If a metadata map is provided, proceed with batch upload
        if 'metadata_map' in request.form:
            metadata_map_str = request.form.get('metadata_map')
            try:
                metadata_map = json.loads(metadata_map_str)
            except json.JSONDecodeError:
                return {'error': "Invalid JSON in 'metadata_map' field"}, 400

            for file in files:
                filename = file.filename
                metadata = metadata_map.get(filename, {}) # Get this file's specific metadata

                title = metadata.get('title', filename)
                uploaded_by = metadata.get('uploaded_by', 'user123')
                drone_id = metadata.get('drone_id', 'unknown_drone')

                try:
                    artifact_id, location, public_url = self.upload_file_to_kafka(
                        file, filename, title, uploaded_by, drone_id
                    )
                    uploaded_artifacts.append({
                        "artifact_id": artifact_id,
                        "drone_id": drone_id,
                        "location": location,
                        "filename": filename,
                        "public_url": public_url,
                        "title": title
                    })
                except Exception as e:
                    app.logger.error(f"Failed to upload file {filename}: {str(e)}")

        # If no metadata map provided, assume single-file upload
        else:
            title = request.form.get('title', '')
            uploaded_by = request.form.get('uploaded_by', 'user123')
            drone_id = request.form.get('drone_id', 'unknown_drone')

            for file in files:
                filename = file.filename

                try:
                    artifact_id, location, public_url = self.upload_file_to_kafka(
                        file, filename, (title or filename), uploaded_by, drone_id
                    )
                    uploaded_artifacts.append({
                        "artifact_id": artifact_id,
                        "drone_id": drone_id,
                        "location": location,
                        "filename": filename,
                        "public_url": public_url,
                        "filename": filename,
                        "title": (title or filename)

                    })
                except Exception as e:
                    app.logger.error(f"Failed to upload file {filename}: {str(e)}")

        if not uploaded_artifacts:
            return {'error': 'All file uploads in the batch failed'}, 500

        return {
            "message": f"Successfully processed and uploaded {len(uploaded_artifacts)} file(s)",
            "uploaded_files": uploaded_artifacts
        }, 201


    def upload_file_to_kafka(self, file, filename, title, uploaded_by, drone_id):
        """
        A helper function to upload one file and send its Kafka messages.
        (This is the logic that was repeated in our if/else block)
        """
        artifact_id = str(uuid.uuid4())
        timestamp_millis = int(datetime.now(timezone.utc).timestamp() * 1000)

        # 1. Upload to MinIO
        file_data = file.read()
        file_stream = io.BytesIO(file_data)
        object_name = f"{artifact_id}/{filename}"

        minio_client.put_object(
            MINIO_BUCKET,
            object_name,
            file_stream,
            len(file_data),
            content_type=file.content_type or 'application/octet-stream'
        )

        # 2. Create metadata record
        location = f"s3://{MINIO_BUCKET}/{object_name}"
        public_url = build_public_url(MINIO_BUCKET, object_name)
        artifact_record = {
            "artifact_id": artifact_id,
            "title": title,
            "filename": filename,
            "location": location,
            "public_url": public_url,
            "uploaded_by": uploaded_by,
            "timestamp": timestamp_millis,
            "drone_id": drone_id
        }

        # 3. Send metadata to 'artifacts' topic
        if not send_to_kafka_with_schema(ARTIFACTS_TOPIC, artifact_id, artifact_record):
            # Raise an error to be caught by the 'post' method
            raise Exception(f"Failed to send artifact {artifact_id} to Kafka")

        # 4. Send notification
        notification_event = {
            "artifact_id": artifact_id,
            "event_type": "artifact_uploaded",
            "event_timestamp": datetime.now(timezone.utc).isoformat()
        }
        send_to_kafka_simple(ARTIFACT_UPLOADED_TOPIC, artifact_id, notification_event)

        # 5. Return the new ID and location
        return artifact_id, location, public_url

    def get(self):
        """Handle GET requests to fetch all artifacts,
        with optional filtering and field selection."""

        conn = None
        try:

            # Handle requested fields
            filters = request.args.copy()
            sql_select = "SELECT * FROM artifacts" # Default to selecting everything

            if 'fields' in filters:
                field_list_str = filters.pop('fields')

                # Sanitize SQL query for safety
                safe_fields = [f for f in field_list_str.split(',') if f.replace('_', '').isalnum()]
                if safe_fields:
                    sql_select = f"SELECT {', '.join(safe_fields)} FROM artifacts"

            # Handle filtering
            sql_query = sql_select
            query_params = []

            if filters:
                sql_query += " WHERE "
                filter_clauses = []

                for key, value in filters.items():
                    if key in ['drone_id', 'uploaded_by', 'title']:
                        filter_clauses.append(f"{key} = %s")
                        query_params.append(value)

                sql_query += " AND ".join(filter_clauses)

            # Connect to Postgres
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            cur = conn.cursor()

            # Execute full query after filtering
            cur.execute(sql_query, query_params)
            artifacts = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            cur.close()
            conn.close()

            # Format into JSON
            result = []
            for row in artifacts:
                result.append(dict(zip(colnames, row)))
            return jsonify(result)

        except Exception as e:
            if conn:
                conn.close()
            return {'error': str(e)}, 500

class ArtifactItemResource(Resource):
    # Authentication
    method_decorators = [require_api_key]

    def get(self, artifact_id):
        """Handle GET requests for a single artifact by its ID."""
        conn = None
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            cur = conn.cursor()

            # Only select single item
            cur.execute("SELECT * FROM artifacts WHERE artifact_id = %s", (artifact_id,))

            # Fetch only one row
            artifact = cur.fetchone()

            cur.close()
            conn.close()

            if artifact:
                # If we found it, format it as JSON (like before)
                colnames = [desc[0] for desc in cur.description]
                result = dict(zip(colnames, artifact))
                return jsonify(result)
            else:
                # If no row was found, return a 404 error
                return {'error': 'Artifact not found'}, 404

        except Exception as e:
            if conn:
                conn.close()
            return {'error': str(e)}, 500

# Register API routes
api.add_resource(ArtifactResource, '/artifacts')
api.add_resource(ArtifactItemResource, '/artifacts/<string:artifact_id>')

@app.route('/health')
def health_check():
    """Return API health status with current timestamp."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat()
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
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

app = Flask(__name__)
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
    'specs_route': '/swagger',
    'headers': []
})
api = Api(app)

# MinIO configuration for object storage
MINIO_ENDPOINT = 'minio:9000'  # Use Docker service name
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'artifacts'

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
    except S3Error as e:
        print(f"Error creating bucket: {e}")

init_minio_bucket()

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
    def post(self):
        """Handle file upload and publish artifact metadata to Kafka."""
        if 'file' not in request.files:
            return {'error': 'No file provided'}, 400
        
        file = request.files['file']
        if file.filename == '':
            return {'error': 'No file selected'}, 400

        # Extract metadata from request
        title = request.form.get('title', '')
        uploaded_by = request.form.get('uploaded_by', 'user123')
        filename = request.form.get('filename', file.filename)

        # Generate unique artifact ID and timestamp
        artifact_id = str(uuid.uuid4())
        timestamp_millis = int(datetime.now(timezone.utc).timestamp() * 1000)

        try:
            # Upload file to MinIO
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

            # Create artifact metadata
            location = f"s3://{MINIO_BUCKET}/{object_name}"
            artifact_record = {
                "artifact_id": artifact_id,
                "title": title or filename,
                "filename": filename,
                "location": location,
                "uploaded_by": uploaded_by,
                "timestamp": timestamp_millis
            }

            # Send metadata to Kafka artifacts topic
            if not send_to_kafka_with_schema(ARTIFACTS_TOPIC, artifact_id, artifact_record):
                return {'error': 'Failed to send artifact to Kafka'}, 500

            # Send notification to artifact_uploaded topic
            notification_event = {
                "artifact_id": artifact_id,
                "event_type": "artifact_uploaded",
                "event_timestamp": datetime.now(timezone.utc).isoformat()
            }
            send_to_kafka_simple(ARTIFACT_UPLOADED_TOPIC, artifact_id, notification_event)

            return {
                "message": "Artifact uploaded successfully",
                "artifact_id": artifact_id,
                "location": location,
                "filename": filename,
                "title": title or filename
            }, 201

        except S3Error as e:
            return {'error': f'MinIO upload failed: {str(e)}'}, 500
        except Exception as e:
            return {'error': f'Upload failed: {str(e)}'}, 500

# Serve the static OpenAPI spec at /swagger.json
@app.route('/swagger.json')
def swagger_spec():
    return send_from_directory('static', 'swagger.json')

# Register API routes
api.add_resource(ArtifactResource, '/artifacts')

@app.route('/health')
def health_check():
    """Return API health status with current timestamp."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat()
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
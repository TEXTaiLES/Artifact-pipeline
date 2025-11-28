import os
import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')

# Topics
ARTIFACTS_TOPIC = 'artifacts'
SENSOR_READINGS_TOPIC = 'sensor_readings'
ARTIFACT_UPLOADED_TOPIC = 'artifact_uploaded'
SENSOR_READING_UPLOADED_TOPIC = 'sensor_reading_uploaded'
# New topics for future features
IMAGE_CAPTURES_TOPIC = 'image_captures'
RECONSTRUCTIONS_TOPIC = 'reconstructions'

# Clients
simple_producer = Producer({'bootstrap.servers': KAFKA_BROKER})
schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

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
        print(f"Sent Avro message to {topic}: {key}")
        return True
    except Exception as e:
        print(f"Error sending Avro to Kafka: {e}")
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
        print(f"Sent simple message to {topic}: {key}")
        return True
    except Exception as e:
        print(f"Error sending simple Kafka msg: {e}")
        return False
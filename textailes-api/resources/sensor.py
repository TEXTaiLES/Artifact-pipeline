from flask import request
from flask_restful import Resource
from datetime import datetime, timezone

from utils import (
    require_api_key,
    send_to_kafka_with_schema,
    send_to_kafka_simple,
    SENSOR_READINGS_TOPIC,
    SENSOR_READING_UPLOADED_TOPIC
)

class SensorReadingResource(Resource):
    method_decorators = [require_api_key]

    def post(self):
        """
        Receive a single sensor reading, publish to Kafka, and notify listeners.
        """
        data = request.get_json()
        if not data:
            return {'error': 'No data provided'}, 400

        # Validate
        #  --- Flexibility in requirements ---
        required_fields = ['temperature', 'humidity', 'sensor_id']
        if not all(k in data for k in required_fields):
             return {'error': f'Missing required fields. Must include: {required_fields}'}, 400

        if 'timestamp' not in data:
             data['timestamp'] = datetime.now(timezone.utc).isoformat()

        message_key = f"{data['sensor_id']}_{data['timestamp']}"

        # Schema definition
        sensor_schema = [
            {"type": "float", "optional": False, "field": "temperature"},
            {"type": "float", "optional": False, "field": "humidity"},
            {"type": "float", "optional": True, "field": "uv_intensity"},
            {"type": "float", "optional": True, "field": "luminosity"},
            {"type": "int32", "optional": True, "field": "atmospheric_pressure"},
            {"type": "float", "optional": True, "field": "elevation"},
            {"type": "string", "optional": False, "field": "timestamp"},
            {"type": "string", "optional": False, "field": "sensor_id"},
            {"type": "string", "optional": True, "field": "artifact_id"}
        ]

        # 1. Send Data to Kafka (Storage)
        success = send_to_kafka_with_schema(
            SENSOR_READINGS_TOPIC,
            message_key,
            data,
            sensor_schema
        )

        if success:
            notification_event = {
                "sensor_id": data['sensor_id'],
                "event_type": "sensor_reading_received",
                "event_timestamp": datetime.now(timezone.utc).isoformat()
            }
            send_to_kafka_simple(SENSOR_READING_UPLOADED_TOPIC, message_key, notification_event)

            return {'message': 'Sensor reading received', 'id': message_key}, 201
        else:
            return {'error': 'Failed to process reading'}, 500
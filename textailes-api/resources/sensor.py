from flask import request, jsonify
from flask_restful import Resource
from datetime import datetime, timezone
import json

from utils import (
    require_api_key,
    send_to_kafka_with_schema,
    send_to_kafka_simple,
    get_db_connection,
    SENSOR_READINGS_TOPIC,
    SENSOR_READING_UPLOADED_TOPIC
)

class SensorReadingResource(Resource):
    method_decorators = [require_api_key]

    def get(self):

        conn = None
        try:
            sensor_id = request.args.get('sensor_id')
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')

            try:
                page = int(request.args.get('page', 1))
                per_page = int(request.args.get('per_page', 50))
            except ValueError:
                return {'error': 'Page and per_page must be integers'}, 400

            offset = (page - 1) * per_page

            sql = "SELECT * FROM sensor_readings WHERE 1=1"
            params = []

            if sensor_id:
                sql += " AND sensor_id = %s"
                params.append(sensor_id)

            if start_date:
                sql += " AND timestamp >= %s"
                params.append(start_date)

            if end_date:
                sql += " AND timestamp <= %s"
                params.append(end_date)

            sql += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
            params.extend([per_page, offset])

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(sql, tuple(params))

            rows = cur.fetchall()

            if cur.description:
                colnames = [desc[0] for desc in cur.description]
                results = []
                for row in rows:
                    row_dict = {}
                    for col, val in zip(colnames, row):
                        if isinstance(val, datetime):
                            row_dict[col] = val.isoformat()
                        else:
                            row_dict[col] = val
                    results.append(row_dict)
            else:
                results = []

            cur.close()
            conn.close()

            return jsonify(results)

        except Exception as e:
            if conn:
                conn.close()
            return {'error': str(e)}, 500

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
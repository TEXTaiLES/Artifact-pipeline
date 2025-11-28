from confluent_kafka import Consumer, KafkaError
import json
from datetime import datetime, timezone
import logging
import os

from services.database import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
ARTIFACT_UPLOADED_TOPIC = 'artifact_uploaded'
GROUP_ID = 'artifact-notification-consumer'


def handle_artifact_uploaded_notification(data):
    """Update the timestamp_update column for an artifact in the database."""
    artifact_id = data.get('artifact_id')
    if not artifact_id:
        logger.warning("No artifact_id in notification message")
        return

    try:
        conn = get_db_connection()

        with conn.cursor() as cur:
            timestamp_update = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE artifacts
                SET timestamp_update = %s
                WHERE artifact_id = %s
                """,
                (timestamp_update, artifact_id)
            )

            if cur.rowcount == 0:
                logger.warning(f"Artifact {artifact_id} not found for timestamp update")
            else:
                conn.commit()
                logger.info(f"Updated timestamp_update for artifact: {artifact_id}")

        conn.close()

    except Exception as e:
        logger.error(f"Error updating timestamp: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()

def consume_artifact_notifications():
    """Consume notification messages from the artifact_uploaded Kafka topic."""

    # Initialize Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })

    consumer.subscribe([ARTIFACT_UPLOADED_TOPIC])

    logger.info(f"Starting Notification Consumer for topic: {ARTIFACT_UPLOADED_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            topic = msg.topic()
            key = msg.key().decode('utf-8') if msg.key() else 'unknown'
            value = msg.value().decode('utf-8') if msg.value() else None

            logger.info(f"Received notification - Topic: {topic}, Key: {key}")

            if not value:
                continue

            try:
                data = json.loads(value)
                handle_artifact_uploaded_notification(data)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
            except Exception as e:
                logger.error(f"Error processing notification: {e}")

    except KeyboardInterrupt:
        logger.info("Notification Consumer stopped by user")
    finally:
        consumer.close()
        logger.info("Notification Consumer closed")

if __name__ == '__main__':
    consume_artifact_notifications()
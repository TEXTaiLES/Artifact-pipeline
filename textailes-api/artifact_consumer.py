from confluent_kafka import Consumer, KafkaError
import json
import psycopg2
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = 'kafka:29092'
ARTIFACT_UPLOADED_TOPIC = 'artifact_uploaded'
GROUP_ID = 'artifact-notification-consumer'

# PostgreSQL configuration
PG_HOST = 'postgres'
PG_PORT = '5432'
PG_DB = 'mydb'
PG_USER = 'admin'
PG_PASSWORD = 'admin123'

def ensure_timestamp_update_column():
    """
    Ensure the timestamp_update column exists in the artifacts table.
    
    Adds the column if it doesn't exist using ALTER TABLE.
    """
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        
        with conn.cursor() as cur:
            # Check if timestamp_update column exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='artifacts' AND column_name='timestamp_update'
            """)
            
            if not cur.fetchone():
                # Add column if it doesn't exist
                cur.execute("""
                    ALTER TABLE artifacts 
                    ADD COLUMN timestamp_update TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                """)
                conn.commit()
                logger.info("Added timestamp_update column to artifacts table")
            else:
                logger.info("timestamp_update column already exists")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error ensuring timestamp_update column: {e}")
        if 'conn' in locals():
            conn.close()

def handle_artifact_uploaded_notification(data):
    """
    Update the timestamp_update column for an artifact in the database.
    
    Args:
        data (dict): Notification data containing artifact_id.
    """
    artifact_id = data.get('artifact_id')
    if not artifact_id:
        logger.warning("No artifact_id in notification message")
        return
    
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        
        with conn.cursor() as cur:
            # Update timestamp_update field
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
        if 'conn' in locals():
            conn.rollback()
            conn.close()

def consume_artifact_notifications():
    """
    Consume notification messages from the artifact_uploaded Kafka topic.
    
    Updates the timestamp_update column in the database for each notification.
    """
    # Ensure timestamp_update column exists
    ensure_timestamp_update_column()
    
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
                logger.warning(f"Empty value for key {key}")
                continue
                
            try:
                data = json.loads(value)
                logger.info(f"Notification data: {json.dumps(data, indent=2)}")
                handle_artifact_uploaded_notification(data)
                    
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
            except Exception as e:
                logger.error(f"Error processing notification: {e}")
                
    except KeyboardInterrupt:
        logger.info("Notification Consumer stopped by user")
    except Exception as e:
        logger.error(f"Notification Consumer error: {e}")
    finally:
        consumer.close()
        logger.info("Notification Consumer closed")

if __name__ == '__main__':
    consume_artifact_notifications()
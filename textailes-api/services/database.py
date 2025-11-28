import os
import psycopg2

# Configuration
PG_HOST = os.environ.get('PG_HOST', 'postgres')
PG_PORT = os.environ.get('PG_PORT', '5432')
PG_DB = os.environ.get('PG_DB')
PG_USER = os.environ.get('PG_USER')
PG_PASSWORD = os.environ.get('PG_PASSWORD')

def get_db_connection():
    """Creates and returns a new connection to the Postgres database."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise e
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.database import get_db_connection
from services.storage import init_minio_bucket, set_public_read_policy

def wait_for_postgres(retries=10, delay=2):
    """Polls Postgres until it is ready."""
    print("Waiting for Postgres...")
    for i in range(retries):
        try:
            conn = get_db_connection()
            conn.close()
            print("Postgres is ready!")
            return True
        except Exception:
            print(f"Postgres not ready yet... ({i+1}/{retries})")
            time.sleep(delay)
    return False

def run_migrations():
    """Runs database schema changes."""
    print("Running migrations...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1. Add 'timestamp_update' to artifacts if missing
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='artifacts' AND column_name='timestamp_update'
        """)
        if not cur.fetchone():
            print("Migration: Adding 'timestamp_update' column to artifacts table.")
            cur.execute("""
                ALTER TABLE artifacts
                ADD COLUMN timestamp_update TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            """)
        else:
            print("Migration: 'timestamp_update' column already exists.")

        conn.commit()
        cur.close()
        conn.close()
        print("Migrations complete.")
    except Exception as e:
        print(f"Migration failed: {e}")

def setup_minio():
    """Initializes MinIO buckets and policies."""
    print("Setting up MinIO...")
    init_minio_bucket()
    set_public_read_policy()
    print("MinIO setup complete.")

if __name__ == "__main__":
    if wait_for_postgres():
        run_migrations()
        setup_minio()
    else:
        print("Could not connect to database. Setup aborted.")
        sys.exit(1)
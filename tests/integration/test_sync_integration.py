import os
import uuid
import pytest
import psycopg2
from dotenv import load_dotenv
from analyzer import sync

load_dotenv()


@pytest.mark.sync
def test_sync_to_postgres(tmp_path):
    """Ensure sync_once can store data to PostgreSQL with preprocessing, and clean up test table."""
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", 5432),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "dbname": os.getenv("DB_NAME", "postgres")
    }

    db_url = f"postgres://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    table_name = f"test_sync_{uuid.uuid4().hex[:8]}"
    last_sync_file = tmp_path / ".last_sync"

    try:
        sync.sync_once(
            start_days_ago=1,
            filters=None,
            output_file=None,
            output_format="none",
            last_sync_file=str(last_sync_file),
            db_url=db_url,
            db_table=table_name,
            preprocess=True
        )

        assert last_sync_file.exists(), "Sync should save last sync timestamp"

        # Optionally verify some data was inserted
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                count = cur.fetchone()[0]
                assert count > 0, f"Expected data in table '{table_name}', but it is empty"

    finally:
        # Clean up: drop the test table
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.commit()

import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

logger = logging.getLogger("DataStorage")
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("data_storage.log", mode="a", encoding="utf-8")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Known column type hints — extend this as needed
SCHEMA_HINTS = {
    "publication-number": "TEXT PRIMARY KEY",
    "tender-value": "NUMERIC",
    "TVH": "NUMERIC",
    "tender-value-lowest": "NUMERIC",
    "buyer-country": "TEXT",
    "notice-type": "TEXT",
    "contract-nature": "TEXT",
    "main-activity": "TEXT",
    "dispatch-date": "TIMESTAMP",
    "publication-date": "TIMESTAMP"
}


def store_dataframe_to_postgres(df: pd.DataFrame, table_name: str, db_config: dict):
    conn = None
    try:
        # Drop rows with missing publication-number (important for PRIMARY KEY)
        if "publication-number" in df.columns:
            before = len(df)
            df = df[df["publication-number"].notna()]
            after = len(df)
            if before != after:
                logger.warning(
                    f"Dropped {before - after} rows with missing 'publication-number'")

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Step 1: Build full CREATE TABLE statement
        column_defs = []
        for col in df.columns:
            pg_type = SCHEMA_HINTS.get(col, "TEXT")
            column_defs.append(f'"{col}" {pg_type}')
        schema_sql = ", ".join(column_defs)

        logger.info(f"Ensuring table '{table_name}' exists with schema.")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql});
        """)

        # Step 2: Get existing column names
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name,))
        existing_columns = set(row[0] for row in cursor.fetchall())

        # Step 3: Add missing columns
        for col in df.columns:
            if col not in existing_columns:
                col_type = SCHEMA_HINTS.get(col, "TEXT")
                logger.warning(
                    f"Column '{col}' missing in DB — adding as {col_type}.")
                cursor.execute(
                    f'ALTER TABLE {table_name} ADD COLUMN "{col}" {col_type};')

        # Step 4: Insert data in batches
        rows = [tuple(str(val) if pd.notna(val) else None for val in row)
                for row in df.to_numpy()]
        quoted_cols = ", ".join(f'"{col}"' for col in df.columns)
        query = f'INSERT INTO {table_name} ({quoted_cols}) VALUES %s'

        logger.info(f"Inserting {len(rows)} rows into '{table_name}'.")
        batch_size = 50000
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i+batch_size]
            try:
                execute_values(cursor, query, chunk)
                logger.info(f"Inserted batch {i // batch_size + 1}")
            except Exception as batch_error:
                logger.error(
                    f"Error in batch {i // batch_size + 1}: {batch_error}")
                conn.rollback()
                raise

        conn.commit()
        cursor.close()
        logger.info(f"Finished storing data to table '{table_name}'.")

    except Exception as e:
        logger.error(f"Error storing DataFrame to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

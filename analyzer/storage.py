import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


def store_dataframe_to_postgres(df: pd.DataFrame, table_name: str, db_config: dict):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Ensure table exists with all required columns
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(f'"{col}" TEXT' for col in df.columns)});
        """)

        # Check existing columns
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name,))
        existing_columns = set(row[0] for row in cursor.fetchall())

        # Add missing columns
        for col in df.columns:
            if col not in existing_columns:
                cursor.execute(
                    f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT;')

        # Insert data as text rows
        rows = [tuple(str(val) for val in row) for row in df.to_numpy()]
        columns_quoted = ", ".join(f'"{col}"' for col in df.columns)
        query = f'INSERT INTO {table_name} ({columns_quoted}) VALUES %s'
        execute_values(cursor, query, rows)

        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"[storage] Error: {e}")
    finally:
        if conn:
            conn.close()

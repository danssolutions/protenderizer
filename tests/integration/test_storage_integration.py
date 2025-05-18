import os
import uuid
import psycopg2
import pandas as pd
import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine
from analyzer.storage import store_dataframe_to_postgres

load_dotenv()


@pytest.mark.storage
def test_store_dataframe_roundtrip():
    """Insert a DataFrame and validate it was stored correctly in PostgreSQL."""
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", 5432),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "dbname": os.getenv("DB_NAME", "postgres")
    }

    table_name = f"test_table_{uuid.uuid4().hex[:8]}"
    df_in = pd.DataFrame([
        {"publication-number": "test-1", "foo": "alpha", "bar": "123"},
        {"publication-number": "test-2", "foo": "beta", "bar": "456"}
    ])

    try:
        # Call the actual storage function
        store_dataframe_to_postgres(df_in, table_name, db_config)

        # Build SQLAlchemy connection string
        engine_str = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?connect_timeout=5"
        )
        engine = create_engine(engine_str)

        # Read the results using SQLAlchemy connection
        df_out = pd.read_sql(
            f'SELECT * FROM "{table_name}" ORDER BY "foo"', engine)

        # Validate structure
        assert df_out.shape == df_in.shape
        assert set(df_out.columns) == set(df_in.columns)

        # Validate content
        for i in range(len(df_in)):
            assert df_out.iloc[i]["foo"] == df_in.iloc[i]["foo"]
            assert df_out.iloc[i]["bar"] == df_in.iloc[i]["bar"]

    finally:
        # Clean up test table
        with psycopg2.connect(connect_timeout=5, **db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.commit()

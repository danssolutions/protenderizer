# import os
# import uuid
# import time
# import pandas as pd
# import psycopg2
# import pytest
# import numpy as np
# from dotenv import load_dotenv
# from sqlalchemy import create_engine, text

# load_dotenv()


# @pytest.mark.performance
# def test_read_preprocessed_notices_within_2s():
#     """Insert a large dataset and verify reading performance from PostgreSQL."""

#     db_config = {
#         "host": os.getenv("DB_HOST", "localhost"),
#         "port": os.getenv("DB_PORT", 5432),
#         "user": os.getenv("DB_USER", "postgres"),
#         "password": os.getenv("DB_PASSWORD", "postgres"),
#         "dbname": os.getenv("DB_NAME", "postgres"),
#     }

#     table_name = f"perf_test_{uuid.uuid4().hex[:8]}"

#     engine_url = (
#         f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
#         f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?connect_timeout=5"
#     )
#     engine = create_engine(engine_url)

#     try:
#         # Insert synthetic test data if table doesn't exist
#         with engine.connect() as conn:
#             table_exists = conn.execute(
#                 text("SELECT to_regclass(:name)"), {"name": table_name}
#             ).scalar()

#         if not table_exists:
#             print(f"Inserting synthetic data into '{table_name}'...")
#             dates = pd.date_range("2010-01-01", periods=100_000, freq="h")
#             df = pd.DataFrame({
#                 "publication-date": dates,
#                 "publication-number": range(100_000),
#                 "tender-value": np.random.randint(1000, 1_000_000, size=100_000),
#                 "notice-type_cn-standard": np.random.randint(0, 2, size=100_000),
#                 "contract-nature_works": np.random.randint(0, 2, size=100_000),
#                 "main-activity_8": np.random.randint(0, 2, size=100_000),
#             })
#             df.to_sql(table_name, con=engine, index=False, if_exists="replace")

#         # Measure read time
#         query = text(
#             f"""
#             SELECT * FROM "{table_name}"
#             WHERE "publication-date" BETWEEN '2015-01-01' AND '2015-12-31'
#             """
#         )

#         start = time.monotonic()
#         with engine.connect() as conn:
#             df = pd.read_sql(query, con=conn)
#         elapsed = time.monotonic() - start

#         assert elapsed < 2.0, f"Query took too long: {elapsed:.2f}s"
#         assert not df.empty, "Query returned no rows"

#     finally:
#         # Clean up the test table
#         with psycopg2.connect(**db_config) as conn:
#             with conn.cursor() as cur:
#                 cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
#             conn.commit()

import os
import uuid
import time
import numpy as np
import pandas as pd
import psycopg2
import pytest
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def generate_synthetic_notices_df(n_rows: int) -> pd.DataFrame:
    """Generate synthetic notice data approximating production schema."""
    dates = pd.date_range("2010-01-01", periods=n_rows, freq="h")
    data = {
        "publication-date": dates,
        "publication-number": [f"PN-{i}" for i in range(n_rows)],
        "tender-value": np.random.randint(10_000, 1_000_000, size=n_rows),
        "tender-value-lowest": np.random.randint(5_000, 500_000, size=n_rows),
    }

    notice_types = [
        "Others", "can-modif", "can-social", "can-standard", "cn-social",
        "cn-standard", "corr", "pin-only", "pin-rtl", "veat"
    ]
    contract_natures = [
        "0", "1", "12", "2", "24", "3", "Others", "services", "supplies", "works"
    ]
    main_activities = [
        "8", "Others", "defence", "education", "electricity", "env-pro",
        "gen-pub", "hc-am", "health", "pub-os", "rail"
    ]
    buyer_countries = [
        "BGR", "CZE", "DEU", "ESP", "FRA", "GBR", "ITA",
        "Others", "POL", "ROU", "SWE"
    ]

    for cat in notice_types:
        data[f"notice-type_{cat}"] = np.random.randint(0, 2, size=n_rows)
    for cat in contract_natures:
        data[f"contract-nature_{cat}"] = np.random.randint(0, 2, size=n_rows)
    for cat in main_activities:
        data[f"main-activity_{cat}"] = np.random.randint(0, 2, size=n_rows)
    for cat in buyer_countries:
        data[f"buyer-country_{cat}"] = np.random.randint(0, 2, size=n_rows)

    return pd.DataFrame(data)


@pytest.mark.performance
def test_read_preprocessed_notices_within_2s():
    """Insert a large dataset and verify reading performance from PostgreSQL."""
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", 5432),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "dbname": os.getenv("DB_NAME", "postgres"),
    }

    table_name = f"perf_test_{uuid.uuid4().hex[:8]}"
    engine_url = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?connect_timeout=5"
    )
    engine = create_engine(engine_url)

    try:
        # Check if table exists
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:name)"), {"name": table_name}
            ).scalar()

        if not exists:
            print(f"Inserting synthetic data into '{table_name}'...")
            df = generate_synthetic_notices_df(100_000)
            df.to_sql(table_name, engine, index=False, if_exists="replace")

        # Run timed query
        query = text(f"""
            SELECT * FROM "{table_name}"
            WHERE "publication-date" BETWEEN '2015-01-01' AND '2015-12-31'
        """)

        start = time.monotonic()
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        elapsed = time.monotonic() - start

        assert elapsed < 2.0, f"Query took too long: {elapsed:.2f}s"
        assert not df.empty, "Query returned no rows"

    finally:
        # Cleanup
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.commit()

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from analyzer import storage


@pytest.mark.storage
def test_store_dataframe_creates_table_if_missing():
    """Ensure CREATE TABLE IF NOT EXISTS is executed with all columns as TEXT."""
    df = pd.DataFrame([
        {"publication-number": "test-001", "col1": "val1", "col2": "val2"}
    ])
    db_config = {"host": "localhost", "user": "test",
                 "password": "test", "dbname": "testdb"}

    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.connection.encoding = "UTF8"
        mock_cursor.mogrify.side_effect = lambda template, args: b"(" + b",".join(
            str(a).encode() for a in args) + b")"
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("col1",)]  # simulate existing column

        storage.store_dataframe_to_postgres(df, "test_table", db_config)

        create_stmt = mock_cursor.execute.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS test_table" in create_stmt
        assert '"col1" TEXT' in create_stmt and '"col2" TEXT' in create_stmt


@pytest.mark.storage
def test_store_dataframe_adds_missing_columns():
    """Verify ALTER TABLE is used to add missing columns."""
    df = pd.DataFrame([
        {"publication-number": "test-002", "a": "x", "b": "y"}
    ])
    db_config = {"host": "localhost", "user": "test",
                 "password": "test", "dbname": "testdb"}

    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.connection.encoding = "UTF8"
        mock_cursor.mogrify.side_effect = lambda template, args: b"(" + b",".join(
            str(a).encode() for a in args) + b")"
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("a",)]  # Only 'a' exists

        storage.store_dataframe_to_postgres(df, "foo", db_config)

        alter_call = any(
            "ALTER TABLE foo ADD COLUMN" in call[0][0] for call in mock_cursor.execute.call_args_list)
        assert alter_call, "Missing columns should trigger ALTER TABLE."


@pytest.mark.storage
def test_store_dataframe_inserts_text_values():
    """Ensure values are stringified before insertion."""
    df = pd.DataFrame([
        {"publication-number": "test-002", "colA": 123, "colB": True}
    ])

    db_config = {"host": "x", "user": "x", "password": "x", "dbname": "x"}

    with patch("psycopg2.connect") as mock_connect, \
            patch("analyzer.storage.execute_values") as mock_exec:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.connection.encoding = "UTF8"
        mock_cursor.mogrify.side_effect = lambda template, args: b"(" + b",".join(
            str(a).encode() for a in args) + b")"
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("colA", "colB")]

        storage.store_dataframe_to_postgres(df, "my_table", db_config)

        # Retrieve the query passed to execute_values
        _, query, values = mock_exec.call_args[0]
        assert 'INSERT INTO my_table ("publication-number", "colA", "colB")' in query
        assert values == [("test-002", "123", "True")]

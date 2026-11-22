
from datetime import datetime
from unittest.mock import MagicMock

from metadata.control_table import ControlTable


def create_control_table_mock(fetch_result):
    """
    Create a ControlTable instance with a mocked Snowflake connection.
    """
    snowflake = MagicMock()

    cursor = snowflake.connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = fetch_result

    control_table = ControlTable(
        snowflake_connector=snowflake,
        database="TEST_DB",
        schema="TEST_SCHEMA",
    )

    return control_table, cursor


def test_get_last_load_time_existing_checkpoint():
    """Return the stored watermark when a checkpoint exists."""

    expected_timestamp = datetime(2026, 11, 20, 10, 30, 0)

    control_table, cursor = create_control_table_mock(
        (expected_timestamp,)
    )

    result = control_table.get_last_load_time("customers")

    assert result == expected_timestamp

    cursor.execute.assert_called_once()

    query = cursor.execute.call_args.args[0]

    assert "TEST_DB.TEST_SCHEMA.control_table" in query
    assert "customers" in query


def test_get_last_load_time_missing_checkpoint():
    """Use the 1970 watermark when no control-table record exists."""

    control_table, cursor = create_control_table_mock(None)

    result = control_table.get_last_load_time("customers")

    assert result == "1970-01-01 00:00:00"

    cursor.execute.assert_called_once()


def test_get_last_load_time_null_checkpoint():
    """Use the 1970 watermark when the stored value is NULL."""

    control_table, cursor = create_control_table_mock((None,))

    result = control_table.get_last_load_time("customers")

    assert result == "1970-01-01 00:00:00"

    cursor.execute.assert_called_once()

def test_mark_success():
    """Verify that a successful load updates the watermark and status."""

    control_table, cursor = create_control_table_mock(None)

    timestamp = datetime(2026, 11, 21, 12, 30, 0)

    control_table.mark_success("customers", timestamp)

    cursor.execute.assert_called_once()

    query = cursor.execute.call_args.args[0]

    assert "UPDATE TEST_DB.TEST_SCHEMA.control_table" in query
    assert "max_etl_update_dttm" in query
    assert "2026-11-21 12:30:00" in query
    assert "last_sync_status = 'PASS'" in query
    assert "Data load successful" in query
    assert "customers" in query


def test_mark_failure():
    """Verify that a failed load is recorded in the control table."""

    control_table, cursor = create_control_table_mock(None)

    control_table.mark_failure(
        "customers",
        "Data load error",
    )

    cursor.execute.assert_called_once()

    query = cursor.execute.call_args.args[0]

    assert "UPDATE TEST_DB.TEST_SCHEMA.control_table" in query
    assert "last_sync_status = 'FAIL'" in query
    assert "Data load error" in query
    assert "customers" in query


def test_mark_no_data():
    """Verify that an empty incremental load is marked successful."""

    control_table, cursor = create_control_table_mock(None)

    control_table.mark_no_data("customers")

    cursor.execute.assert_called_once()

    query = cursor.execute.call_args.args[0]

    assert "UPDATE TEST_DB.TEST_SCHEMA.control_table" in query
    assert "last_sync_status = 'PASS'" in query
    assert "No new data found" in query
    assert "customers" in query

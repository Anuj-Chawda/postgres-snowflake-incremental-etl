import pytest
from unittest.mock import MagicMock, patch

import pandas as pd

from loaders.table_loader import TableLoader


def test_no_new_data():
    """Verify that an empty incremental load skips Snowflake writes."""

    # Mock database connectors and the control table.
    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    postgres.connection = MagicMock()
    control_table.get_last_load_time.return_value = (
        "2026-11-20 10:00:00"
    )

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate PostgreSQL returning no new records.
    with patch(
        "loaders.table_loader.pd.read_sql_query",
        return_value=pd.DataFrame(),
    ) as mock_read_sql:

        loader._load_single_table(
            table="customers",
            pk_keys={"customers": ["customer_id"]},
        )

    # Verify the stored watermark was retrieved.
    control_table.get_last_load_time.assert_called_once_with(
        "customers"
    )

    # Verify the incremental SQL query.
    mock_read_sql.assert_called_once()

    query = mock_read_sql.call_args.args[0]

    assert "FROM public.customers" in query
    assert "etl_update_dttm >" in query
    assert "2026-11-20 10:00:00" in query

    # Verify that an empty load is recorded.
    control_table.mark_no_data.assert_called_once_with(
        "customers"
    )

    # No success or failure status should be recorded.
    control_table.mark_success.assert_not_called()
    control_table.mark_failure.assert_not_called()

    # No Snowflake writes should occur.
    snowflake.connection.cursor.assert_not_called()

def test_successful_incremental_load():
    """Verify that new records are loaded and the watermark advances."""

    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    control_table.get_last_load_time.return_value = (
        "2026-11-20 10:00:00"
    )

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate new records returned by PostgreSQL.
    source_df = pd.DataFrame({
        "customer_id": [101, 102],
        "etl_update_dttm": [
            "2026-11-21 11:00:00",
            "2026-11-21 12:00:00",
        ],
    })

    # Mock the Snowflake operations while testing orchestration.
    with (
        patch(
            "loaders.table_loader.pd.read_sql_query",
            return_value=source_df,
        ),
        patch.object(
            loader,
            "_transform_dataframe",
            return_value=source_df,
        ) as mock_transform,
        patch.object(loader, "_create_temp_table") as mock_create,
        patch.object(loader, "_write_dataframe") as mock_write,
        patch.object(loader, "_merge_into_target") as mock_merge,
    ):

        loader._load_single_table(
            table="customers",
            pk_keys={"customers": ["customer_id"]},
        )

    # Verify the DataFrame was transformed.
    mock_transform.assert_called_once()

    # Verify Snowflake staging operations.
    mock_create.assert_called_once_with(
        "customers", "temp_customers"
    )

    mock_write.assert_called_once()

    mock_merge.assert_called_once_with(
        "customers",
        "temp_customers",
        {"customers": ["customer_id"]},
    )

    # Verify the watermark was updated to the latest record.
    control_table.mark_success.assert_called_once_with(
        "customers",
        "2026-11-21 12:00:00",
    )

    # Verify no incorrect status was recorded.
    control_table.mark_no_data.assert_not_called()
    control_table.mark_failure.assert_not_called()

def test_load_failure_does_not_advance_watermark():
    """
    Verify that a failed table load is recorded without
    updating the successful-load watermark.
    """

    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    postgres.pg_uri = "postgresql://test_user:test_password@localhost/test_db"

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate a failure while loading the customers table.
    with (
        patch(
            "loaders.table_loader.get_primary_keys",
            return_value={"customers": ["customer_id"]},
        ),
        patch(
            "loaders.table_loader.clean_prev_instance_temp_tables",
        ),
        patch.object(
            loader,
            "_load_single_table",
            side_effect=RuntimeError("Simulated Snowflake load failure"),
        ) as mock_load,
    ):
        loader.load_tables()

    # The loader attempted to process the table.
    mock_load.assert_called_once_with(
        "customers",
        {"customers": ["customer_id"]},
    )

    # The failure must be recorded.
    control_table.mark_failure.assert_called_once_with(
        "customers",
        "Data load error",
    )

    # A failed load must never advance the watermark.
    control_table.mark_success.assert_not_called()

    # It must not be incorrectly marked as having no new data.
    control_table.mark_no_data.assert_not_called()

def test_staging_load_failure_prevents_merge_and_checkpoint():
    """
    A failed Snowflake staging load must prevent the target
    merge and successful checkpoint update.
    """

    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    control_table.get_last_load_time.return_value = (
        "2026-11-20 10:00:00"
    )

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    source_df = pd.DataFrame({
        "customer_id": [101, 102],
        "etl_update_dttm": [
            "2026-11-21 11:00:00",
            "2026-11-21 12:00:00",
        ],
    })

    with (
        patch(
            "loaders.table_loader.pd.read_sql_query",
            return_value=source_df,
        ),
        patch.object(
            loader,
            "_transform_dataframe",
            return_value=source_df,
        ),
        patch.object(loader, "_create_temp_table") as mock_create,
        patch.object(
            loader,
            "_write_dataframe",
            side_effect=RuntimeError("Snowflake staging load failed"),
        ) as mock_write,
        patch.object(loader, "_merge_into_target") as mock_merge,
    ):

        with pytest.raises(
            RuntimeError,
            match="Snowflake staging load failed",
        ):
            loader._load_single_table(
                table="customers",
                pk_keys={"customers": ["customer_id"]},
            )

    # Staging was created and the write was attempted.
    mock_create.assert_called_once_with(
        "customers", "temp_customers"
    )
    mock_write.assert_called_once()

    # No target merge after staging failure.
    mock_merge.assert_not_called()

    # Most importantly, the watermark must not advance.
    control_table.mark_success.assert_not_called()
    control_table.mark_no_data.assert_not_called()

def test_write_dataframe_failure():
    """
    Verify that an unsuccessful Snowflake write_pandas()
    result raises RuntimeError.
    """

    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    df = pd.DataFrame({
        "customer_id": [101, 102],
        "customer_name": ["Alice", "Bob"],
    })

    # Simulate write_pandas() reporting an unsuccessful load.
    with patch(
        "loaders.table_loader.write_pandas",
        return_value=(False, 1, 0, []),
    ) as mock_write:

        with pytest.raises(
            RuntimeError,
            match="Snowflake load failed",
        ):
            loader._write_dataframe(
                df,
                "temp_customers",
            )

    # Verify that write_pandas() was called correctly.
    mock_write.assert_called_once_with(
        snowflake.connection,
        df,
        "temp_customers",
        auto_create_table=False,
        quote_identifiers=False,
        on_error="CONTINUE",
    )

def test_write_dataframe_success():
    """
    Verify that a successful Snowflake write_pandas()
    result completes without raising an exception.
    """

    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    df = pd.DataFrame({
        "customer_id": [101, 102],
        "customer_name": ["Alice", "Bob"],
    })

    # Simulate write_pandas() successfully loading two rows.
    with patch(
        "loaders.table_loader.write_pandas",
        return_value=(True, 1, 2, []),
    ) as mock_write:

        result = loader._write_dataframe(
            df,
            "temp_customers",
        )

    # Verify that write_pandas() received the correct arguments.
    mock_write.assert_called_once_with(
        snowflake.connection,
        df,
        "temp_customers",
        auto_create_table=False,
        quote_identifiers=False,
        on_error="CONTINUE",
    )

    # The method should complete normally.
    assert result is None

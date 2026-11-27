import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import OperationalError
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

    # The maximum timestamp is passed into the transactional merge.
    mock_merge.assert_called_once_with(
        "customers",
        "temp_customers",
        {"customers": ["customer_id"]},
        pd.Timestamp("2026-11-21 12:00:00"),
    )

    # The checkpoint is now updated within _merge_into_target(),
    # not directly by _load_single_table().
    control_table.mark_success.assert_not_called()

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
        on_error="ABORT_STATEMENT",
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
        on_error="ABORT_STATEMENT",
    )

    # The method should complete normally.
    assert result is None

def test_transform_dataframe_timestamps():
    """
    Verify that timestamp columns are converted correctly
    and invalid/null values are handled safely.
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
        "customer_id": [101, 102, 103],
        "etl_update_dttm": [
            "2026-11-21 12:00:00",
            None,
            "invalid_timestamp",
        ],
        "customer_name": ["Alice", "Bob", "Charlie"],
    })

    result = loader._transform_dataframe(df.copy())

    # UTC 12:00 becomes New York 07:00 in November.
    assert (
        result.loc[0, "etl_update_dttm"]
        == "2026-11-21 07:00:00.000000"
    )

    # Null timestamps remain missing.
    assert pd.isna(result.loc[1, "etl_update_dttm"])

    # Invalid timestamps are converted to missing values.
    assert pd.isna(result.loc[2, "etl_update_dttm"])

    # Non-timestamp columns remain unchanged.
    assert result["customer_id"].tolist() == [101, 102, 103]

    assert result["customer_name"].tolist() == [
        "Alice",
        "Bob",
        "Charlie",
    ]

def test_transform_dataframe_daylight_saving():
    """
    Verify that UTC-to-New-York conversion correctly
    handles standard time and daylight saving time.
    """

    loader = TableLoader(
        postgres=MagicMock(),
        snowflake=MagicMock(),
        control_table=MagicMock(),
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    df = pd.DataFrame({
        "customer_id": [101, 102],
        "etl_update_dttm": [
            "2026-01-15 12:00:00",
            "2026-07-15 12:00:00",
        ],
    })

    result = loader._transform_dataframe(df.copy())

    # January: Eastern Standard Time (UTC-5).
    assert result.loc[0, "etl_update_dttm"] == (
        "2026-01-15 07:00:00.000000"
    )

    # July: Eastern Daylight Time (UTC-4).
    assert result.loc[1, "etl_update_dttm"] == (
        "2026-07-15 08:00:00.000000"
    )

def test_write_dataframe_row_count_mismatch():
    """
    Verify that the loader raises an exception when Snowflake
    reports fewer loaded rows than the source DataFrame contains.
    """

    loader = TableLoader(
        postgres=MagicMock(),
        snowflake=MagicMock(),
        control_table=MagicMock(),
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    df = pd.DataFrame({
        "customer_id": [101, 102, 103],
        "customer_name": ["Alice", "Bob", "Charlie"],
    })

    # Snowflake reports success but only two of three rows loaded.
    with patch(
        "loaders.table_loader.write_pandas",
        return_value=(True, 1, 2, []),
    ) as mock_write:

        with pytest.raises(
            RuntimeError,
            match="Snowflake row count mismatch",
        ):
            loader._write_dataframe(df, "temp_customers")

    mock_write.assert_called_once_with(
        loader.snowflake.connection,
        df,
        "temp_customers",
        auto_create_table=False,
        quote_identifiers=False,
        on_error="ABORT_STATEMENT",
    )

def test_merge_rollback_on_insert_failure():
    """
    Verify that an INSERT failure triggers ROLLBACK,
    prevents COMMIT, and attempts staging-table cleanup.
    """

    loader = TableLoader(
        postgres=MagicMock(),
        snowflake=MagicMock(),
        control_table=MagicMock(),
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    cursor = (
        loader.snowflake.connection.cursor
        .return_value.__enter__.return_value
    )

    executed_queries = []

    def simulate_execute(query):
        executed_queries.append(query.strip())

        if query.strip().startswith("INSERT INTO"):
            raise RuntimeError("Simulated Snowflake INSERT failure")

    cursor.execute.side_effect = simulate_execute

    with pytest.raises(
        RuntimeError,
        match="Simulated Snowflake INSERT failure",
    ):
        loader._merge_into_target(
            "customers",
            "temp_customers",
            {"customers": ["customer_id"]},
            "2026-11-22 12:00:00",
        )

    # Transaction must begin.
    assert executed_queries[0] == "BEGIN"

    # Both DELETE and INSERT must be attempted.
    assert any(
        query.startswith("DELETE FROM")
        for query in executed_queries
    )

    assert any(
        query.startswith("INSERT INTO")
        for query in executed_queries
    )

    # Failed transaction must be rolled back.
    assert "ROLLBACK" in executed_queries

    # Failed transaction must never be committed.
    assert "COMMIT" not in executed_queries

    # Staging cleanup should still be attempted.
    assert any(
        query.startswith("DROP TABLE IF EXISTS")
        for query in executed_queries
    )

    # Verify the sequence of important operations.
    assert executed_queries.index("BEGIN") < next(
        i for i, query in enumerate(executed_queries)
        if query.startswith("DELETE FROM")
    )

    assert executed_queries.index("ROLLBACK") < next(
        i for i, query in enumerate(executed_queries)
        if query.startswith("DROP TABLE IF EXISTS")
    )

def test_merge_successful_transaction():
    """
    Verify a successful merge commits the transaction
    and cleans up the staging table.
    """

    loader = TableLoader(
        postgres=MagicMock(),
        snowflake=MagicMock(),
        control_table=MagicMock(),
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    cursor = (
        loader.snowflake.connection.cursor
        .return_value.__enter__.return_value
    )

    loader._merge_into_target(
        "customers",
        "temp_customers",
        {"customers": ["customer_id"]},
        "2026-11-22 12:00:00",
    )

    # Confirm that the checkpoint update uses the same transaction cursor.
    loader.control_table.mark_success.assert_called_once_with(
        "customers",
        "2026-11-22 12:00:00",
        cursor=cursor,
    )

    executed_queries = [
        call.args[0].strip()
        for call in cursor.execute.call_args_list
    ]

    # Verify the complete successful transaction sequence.
    assert len(executed_queries) == 5

    assert executed_queries[0] == "BEGIN"
    assert executed_queries[1].startswith("DELETE FROM")
    assert executed_queries[2].startswith("INSERT INTO")
    assert executed_queries[3] == "COMMIT"
    assert executed_queries[4].startswith("DROP TABLE IF EXISTS")

    # A successful merge must not trigger rollback.
    assert "ROLLBACK" not in executed_queries

    # Verify the expected target and staging tables.
    assert "TEST_DB.TEST_SCHEMA.customers" in executed_queries[1]
    assert "TEST_DB.TEST_SCHEMA.temp_customers" in executed_queries[2]

def test_merge_rollback_on_checkpoint_failure():
    """
    Verify that a checkpoint update failure triggers rollback
    of the target-table transaction.
    """

    loader = TableLoader(
        postgres=MagicMock(),
        snowflake=MagicMock(),
        control_table=MagicMock(),
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    cursor = (
        loader.snowflake.connection.cursor
        .return_value.__enter__.return_value
    )

    # Simulate the checkpoint update failing after DELETE and INSERT.
    loader.control_table.mark_success.side_effect = RuntimeError(
        "Simulated checkpoint update failure"
    )

    with pytest.raises(
        RuntimeError,
        match="Simulated checkpoint update failure",
    ):
        loader._merge_into_target(
            "customers",
            "temp_customers",
            {"customers": ["customer_id"]},
            "2026-11-22 12:00:00",
        )

    executed_queries = [
        call.args[0].strip()
        for call in cursor.execute.call_args_list
    ]

    # Verify DELETE and INSERT were attempted.
    assert executed_queries[0] == "BEGIN"
    assert executed_queries[1].startswith("DELETE FROM")
    assert executed_queries[2].startswith("INSERT INTO")

    # The checkpoint update should use the transaction cursor.
    loader.control_table.mark_success.assert_called_once_with(
        "customers",
        "2026-11-22 12:00:00",
        cursor=cursor,
    )

    # Failed checkpoint update must cause rollback.
    assert "ROLLBACK" in executed_queries

    # No commit must occur.
    assert "COMMIT" not in executed_queries

    # Staging-table cleanup is attempted after rollback.
    assert executed_queries[-1].startswith("DROP TABLE IF EXISTS")

def test_merge_rollback_when_checkpoint_updates_zero_rows():
    """
    Verify that a missing checkpoint record triggers rollback
    of the DELETE and INSERT transaction.
    """
    from metadata.control_table import ControlTable

    snowflake = MagicMock()

    control_table = ControlTable(
        snowflake_connector=snowflake,
        database="TEST_DB",
        schema="TEST_SCHEMA",
    )

    loader = TableLoader(
        postgres=MagicMock(),
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    cursor = (
        snowflake.connection.cursor
        .return_value.__enter__.return_value
    )

    # Simulate an UPDATE that succeeds but affects no records.
    cursor.rowcount = 0

    with pytest.raises(
        RuntimeError,
        match="Checkpoint update failed for customers",
    ):
        loader._merge_into_target(
            "customers",
            "temp_customers",
            {"customers": ["customer_id"]},
            "2026-11-26 12:00:00",
        )

    executed_queries = [
        call.args[0].strip()
        for call in cursor.execute.call_args_list
    ]

    assert executed_queries[0] == "BEGIN"
    assert executed_queries[1].startswith("DELETE FROM")
    assert executed_queries[2].startswith("INSERT INTO")

    # The control-table UPDATE must be attempted.
    assert executed_queries[3].startswith(
        "UPDATE TEST_DB.TEST_SCHEMA.control_table"
    )

    # No commit should occur.
    assert "COMMIT" not in executed_queries

    # The transaction must be rolled back.
    assert executed_queries[4] == "ROLLBACK"

    # Staging cleanup should happen after rollback.
    assert executed_queries[5].startswith(
        "DROP TABLE IF EXISTS"
    )

def test_postgres_disconnect_triggers_reconnection():
    """
    Verify that a PostgreSQL disconnect is recorded and
    the connection is recovered before loading the next table.
    """

    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    postgres.pg_uri = "postgresql://test_user@localhost/test_db"

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers", "orders"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate an SQLAlchemy-recognized PostgreSQL disconnect.
    disconnect_error = OperationalError(
        statement="SELECT * FROM public.customers",
        params=None,
        orig=Exception("PostgreSQL connection lost"),
        connection_invalidated=True,
    )

    def simulate_table_load(table, pk_keys):
        if table == "customers":
            raise disconnect_error

    with (
        patch(
            "loaders.table_loader.get_primary_keys",
            return_value={
                "customers": ["customer_id"],
                "orders": ["order_id"],
            },
        ),
        patch(
            "loaders.table_loader.clean_prev_instance_temp_tables",
        ),
        patch.object(
            loader,
            "_load_single_table",
            side_effect=simulate_table_load,
        ) as mock_load,
    ):
        loader.load_tables()

    # The failed table should be marked as failed.
    control_table.mark_failure.assert_called_once_with(
        "customers",
        "Data load error",
    )

    # PostgreSQL connection recovery should be attempted.
    postgres.reconnect.assert_called_once_with()

    # Processing should continue to the next table.
    assert mock_load.call_count == 2

    assert mock_load.call_args_list[0].args[0] == "customers"
    assert mock_load.call_args_list[1].args[0] == "orders"

    # The failed table must not receive a successful checkpoint.
    control_table.mark_success.assert_not_called()

def test_snowflake_failure_does_not_reconnect_postgres():
    """
    Verify that a Snowflake loading failure does not trigger
    unnecessary PostgreSQL reconnection.
    """

    postgres = MagicMock()
    snowflake = MagicMock()
    control_table = MagicMock()

    postgres.pg_uri = "postgresql://test_user@localhost/test_db"

    loader = TableLoader(
        postgres=postgres,
        snowflake=snowflake,
        control_table=control_table,
        tables=["customers", "orders"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    def simulate_table_load(table, pk_keys):
        if table == "customers":
            raise RuntimeError("Simulated Snowflake loading failure")

    with (
        patch(
            "loaders.table_loader.get_primary_keys",
            return_value={
                "customers": ["customer_id"],
                "orders": ["order_id"],
            },
        ),
        patch(
            "loaders.table_loader.clean_prev_instance_temp_tables",
        ),
        patch.object(
            loader,
            "_load_single_table",
            side_effect=simulate_table_load,
        ) as mock_load,
    ):
        loader.load_tables()

    # The failed table must be recorded.
    control_table.mark_failure.assert_called_once_with(
        "customers",
        "Data load error",
    )

    # No PostgreSQL reconnection should occur.
    postgres.reconnect.assert_not_called()

    # Processing should continue to the next table.
    assert mock_load.call_count == 2
    assert mock_load.call_args_list[0].args[0] == "customers"
    assert mock_load.call_args_list[1].args[0] == "orders"


from unittest.mock import MagicMock, patch
import pytest
from validation.schema_validator import SchemaValidator


def test_schema_validation_success():
    """
    Verify that validation completes successfully when
    PostgreSQL and Snowflake schemas match.
    """

    postgres = MagicMock()
    snowflake = MagicMock()

    postgres.pg_uri = "postgresql://test_user@localhost/test_db"

    validator = SchemaValidator(
        postgres_connector=postgres,
        snowflake_connector=snowflake,
        tables=["customers", "orders"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate schema_comparison() finding no differences.
    schema_results = {
        "missing_tables_in_snowflake": [],
        "tables_compared": {
            "customers": {
                "missing_columns": [],
                "datatype_mismatches": [],
            },
            "orders": {
                "missing_columns": [],
                "datatype_mismatches": [],
            },
        },
    }

    with patch(
        "validation.schema_validator.schema_comparison",
        return_value=schema_results,
    ) as mock_comparison:

        result = validator.validate()

    # Verify schema_comparison() received the correct arguments.
    mock_comparison.assert_called_once_with(
        postgres.pg_uri,
        snowflake.connection,
        "public",
        "TEST_DB",
        "TEST_SCHEMA",
        ["customers", "orders"],
    )

    # Successful validation returns normally.
    assert result is None

    # No failure updates should be made to Snowflake.
    snowflake.connection.cursor.assert_not_called()

def test_schema_validation_missing_table():
    """
    Verify that a missing Snowflake table is marked as failed
    and schema validation terminates with exit code 1.
    """

    postgres = MagicMock()
    snowflake = MagicMock()

    postgres.pg_uri = "postgresql://test_user@localhost/test_db"

    validator = SchemaValidator(
        postgres_connector=postgres,
        snowflake_connector=snowflake,
        tables=["customers", "orders"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate the customers table being absent from Snowflake.
    schema_results = {
        "missing_tables_in_snowflake": ["customers"],
        "tables_compared": {
            "orders": {
                "missing_columns": [],
                "datatype_mismatches": [],
            },
        },
    }

    cursor = snowflake.connection.cursor.return_value.__enter__.return_value

    with patch(
        "validation.schema_validator.schema_comparison",
        return_value=schema_results,
    ):
        with pytest.raises(SystemExit) as exc_info:
            validator.validate()

    # Schema validation must terminate with exit code 1.
    assert exc_info.value.code == 1

    # Verify the failure was recorded in the Snowflake control table.
    cursor.execute.assert_called_once()

    query = cursor.execute.call_args.args[0]

    assert "UPDATE TEST_DB.TEST_SCHEMA.control_table" in query
    assert "last_sync_status = 'FAIL'" in query
    assert "Table Missing in Snowflake" in query
    assert "WHERE table_name = 'customers'" in query

def test_schema_validation_missing_columns():
    """
    Verify that missing Snowflake columns are recorded as
    a schema mismatch and validation exits with code 1.
    """

    postgres = MagicMock()
    snowflake = MagicMock()

    postgres.pg_uri = "postgresql://test_user@localhost/test_db"

    validator = SchemaValidator(
        postgres_connector=postgres,
        snowflake_connector=snowflake,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate the email column being missing in Snowflake.
    schema_results = {
        "missing_tables_in_snowflake": [],
        "tables_compared": {
            "customers": {
                "missing_columns": ["email"],
                "datatype_mismatches": [],
            },
        },
    }

    cursor = snowflake.connection.cursor.return_value.__enter__.return_value

    with patch(
        "validation.schema_validator.schema_comparison",
        return_value=schema_results,
    ):
        with pytest.raises(SystemExit) as exc_info:
            validator.validate()

    # The pipeline must stop when schemas are incompatible.
    assert exc_info.value.code == 1

    # The Snowflake control table should be updated.
    cursor.execute.assert_called_once()

    query = cursor.execute.call_args.args[0]

    assert "UPDATE TEST_DB.TEST_SCHEMA.control_table" in query
    assert "last_sync_status = 'FAIL'" in query
    assert "Schema Mismatch" in query
    assert "WHERE table_name = 'customers'" in query

def test_schema_validation_datatype_mismatch():
    """
    Verify that a datatype mismatch is recorded in the
    control table and schema validation exits with code 1.
    """

    postgres = MagicMock()
    snowflake = MagicMock()

    postgres.pg_uri = "postgresql://test_user@localhost/test_db"

    validator = SchemaValidator(
        postgres_connector=postgres,
        snowflake_connector=snowflake,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate schema_comparison() reporting a datatype mismatch.
    schema_results = {
        "missing_tables_in_snowflake": [],
        "tables_compared": {
            "customers": {
                "missing_columns": [],
                "datatype_mismatches": [
                    {
                        "column": "customer_id",
                        "postgres_type": "INTEGER",
                        "snowflake_type": "VARCHAR",
                    }
                ],
            },
        },
    }

    cursor = snowflake.connection.cursor.return_value.__enter__.return_value

    with patch(
        "validation.schema_validator.schema_comparison",
        return_value=schema_results,
    ):
        with pytest.raises(SystemExit) as exc_info:
            validator.validate()

    # Validation must terminate with exit code 1.
    assert exc_info.value.code == 1

    # The control table should record the schema mismatch.
    cursor.execute.assert_called_once()

    query = cursor.execute.call_args.args[0]

    assert "UPDATE TEST_DB.TEST_SCHEMA.control_table" in query
    assert "last_sync_status = 'FAIL'" in query
    assert "Schema Mismatch" in query
    assert "WHERE table_name = 'customers'" in query

def test_schema_validation_comparison_error():
    """
    Verify that errors raised during schema comparison
    propagate and validation does not appear successful.
    """

    postgres = MagicMock()
    snowflake = MagicMock()

    postgres.pg_uri = "postgresql://test_user@localhost/test_db"

    validator = SchemaValidator(
        postgres_connector=postgres,
        snowflake_connector=snowflake,
        tables=["customers"],
        pg_schema="public",
        sf_database="TEST_DB",
        sf_schema="TEST_SCHEMA",
    )

    # Simulate a database error during schema comparison.
    with patch(
        "validation.schema_validator.schema_comparison",
        side_effect=RuntimeError("Schema metadata query failed"),
    ) as mock_comparison:

        with pytest.raises(
            RuntimeError,
            match="Schema metadata query failed",
        ):
            validator.validate()

    # Verify the comparison was attempted.
    mock_comparison.assert_called_once_with(
        postgres.pg_uri,
        snowflake.connection,
        "public",
        "TEST_DB",
        "TEST_SCHEMA",
        ["customers"],
    )

    # No control-table update should be attempted by the validator.
    snowflake.connection.cursor.assert_not_called()


import ast
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from cryptography.hazmat.primitives import serialization

from config.config_loader import ConfigLoader
from connectors.snowflake_connector import SnowflakeConnector
from metadata.control_table import ControlTable
from loaders.table_loader import TableLoader
from lib.decryption import decrypt_data


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASE = "ICEDQ_DB"
SCHEMA = "ETL_INTEGRATION_TEST"
TABLE = "etl_integration_test"

TARGET = f"{DATABASE}.{SCHEMA}.{TABLE}"
CONTROL = f"{DATABASE}.{SCHEMA}.CONTROL_TABLE"

EXPECTED_CHECKPOINT = datetime(2026, 11, 28, 10, 0, 0)


def get_baseline(connection):
    """Read the target data and checkpoint."""

    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM {TARGET}"
        )
        row_count = cursor.fetchone()[0]

        cursor.execute(
            f"SELECT CUSTOMER_NAME FROM {TARGET} "
            "WHERE CUSTOMER_ID = 102"
        )
        customer_name = cursor.fetchone()[0]

        cursor.execute(
            f"SELECT MAX_ETL_UPDATE_DTTM FROM {CONTROL} "
            "WHERE TABLE_NAME = 'etl_integration_test'"
        )
        checkpoint = cursor.fetchone()[0]

    return row_count, customer_name, checkpoint


def main():

    config = ConfigLoader.load(
        str(PROJECT_ROOT / "config.integration.yaml")
    )

    # Restrict this script to the isolated test environment.
    if (
        config["pg_host"].lower() not in ("localhost", "127.0.0.1")
        or config["pg_database"].lower() != "etl_test_db"
        or config["sf_database"].upper() != DATABASE
        or config["sf_schema"].upper() != SCHEMA
        or ast.literal_eval(config["tables"]) != [TABLE]
    ):
        raise RuntimeError("Integration safety checks failed")

    passphrase = decrypt_data(
        config["snow_key_pair_passphrase"]
    )

    if passphrase is None:
        raise RuntimeError("Snowflake passphrase decryption failed")

    with open(PROJECT_ROOT / "rsa_key.p8", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode(),
        )

    snowflake = SnowflakeConnector(config, private_key)
    stage_name = f"temp_etl_rollback_{uuid4().hex[:12]}"
    staging = f"{DATABASE}.{SCHEMA}.{stage_name}"

    try:
        snowflake.connect()
        connection = snowflake.connection

        baseline = get_baseline(connection)

        assert baseline == (
            4,
            "Robert",
            EXPECTED_CHECKPOINT,
        ), f"Unexpected baseline: {baseline}"

        print("PASS: Baseline verified")

        # Create an isolated staging table.
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE {staging} LIKE {TARGET}"
            )

            cursor.execute(
                f"INSERT INTO {staging} "
                "(CUSTOMER_ID, CUSTOMER_NAME, ETL_UPDATE_DTTM) "
                "VALUES (102, 'Rollback Probe', "
                "'2026-11-28 05:00:00')"
            )

        print("PASS: Test staging table prepared")

        control_table = ControlTable(
            snowflake,
            DATABASE,
            SCHEMA,
        )

        loader = TableLoader(
            postgres=None,
            snowflake=snowflake,
            control_table=control_table,
            tables=[TABLE],
            pg_schema="public",
            sf_database=DATABASE,
            sf_schema=SCHEMA,
        )

        def fail_checkpoint(table, timestamp, cursor=None):
            # Verify that DELETE and INSERT really ran
            # inside the still-active transaction.
            cursor.execute(
                f"SELECT CUSTOMER_NAME FROM {TARGET} "
                "WHERE CUSTOMER_ID = 102"
            )

            current_name = cursor.fetchone()[0]

            assert current_name == "Rollback Probe", (
                f"Unexpected transaction state: {current_name}"
            )

            raise RuntimeError(
                "Simulated checkpoint failure"
            )

        # Execute the real merge with one controlled failure.
        with patch.object(
            control_table,
            "mark_success",
            side_effect=fail_checkpoint,
        ) as mock_checkpoint:

            try:
                loader._merge_into_target(
                    TABLE,
                    stage_name,
                    {TABLE: ["customer_id"]},
                    EXPECTED_CHECKPOINT,
                )
            except RuntimeError as exc:
                if str(exc) != "Simulated checkpoint failure":
                    raise
            else:
                raise AssertionError(
                    "Expected checkpoint failure did not occur"
                )

            mock_checkpoint.assert_called_once()

        print("PASS: Checkpoint failure triggered rollback")

        # Verify actual Snowflake data after rollback.
        after_rollback = get_baseline(connection)

        assert after_rollback == baseline, (
            f"Rollback verification failed: {after_rollback}"
        )

        print("PASS: Original target data preserved")
        print("PASS: Watermark preserved")
        print("PASS: Snowflake transaction rollback verified")

    finally:
        if snowflake.connection is not None:
            # Cleanup in case the merge did not finish cleanup.
            with snowflake.connection.cursor() as cursor:
                cursor.execute(
                    f"DROP TABLE IF EXISTS {staging}"
                )

            snowflake.close()


if __name__ == "__main__":
    main()

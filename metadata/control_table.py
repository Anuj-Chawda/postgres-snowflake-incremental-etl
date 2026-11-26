import logging

logger = logging.getLogger(__name__)


class ControlTable:
    """
    Handles all metadata operations related to the control_table
    in Snowflake.
    """

    def __init__(self, snowflake_connector, database, schema):
        self.snowflake = snowflake_connector
        self.database = database
        self.schema = schema

    def get_last_load_time(self, table_name):
        """
        Fetch the last successful sync timestamp for a table.
        """

        query = f"""
            SELECT max_etl_update_dttm
            FROM {self.database}.{self.schema}.control_table
            WHERE table_name = '{table_name}'
        """

        with self.snowflake.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()

        if result and result[0]:
            return result[0]

        return "1970-01-01 00:00:00"

    def mark_no_data(self, table_name):
        """
        Mark table sync as completed with no new data.
        """

        query = f"""
            UPDATE {self.database}.{self.schema}.control_table
            SET last_sync_dttm =
                CONVERT_TIMEZONE('America/Los_Angeles','America/New_York',CURRENT_TIMESTAMP),
                last_sync_status = 'PASS',
                comment = 'No new data found'
            WHERE table_name = '{table_name}'
        """

        with self.snowflake.connection.cursor() as cursor:
            cursor.execute(query)

    def mark_failure(self, table_name, comment):
        """
        Mark table sync as failed.
        """

        query = f"""
            UPDATE {self.database}.{self.schema}.control_table
            SET last_sync_dttm =
                CONVERT_TIMEZONE('America/Los_Angeles','America/New_York',CURRENT_TIMESTAMP),
                last_sync_status = 'FAIL',
                comment = '{comment}'
            WHERE table_name = '{table_name}'
        """

        with self.snowflake.connection.cursor() as cursor:
            cursor.execute(query)

    def mark_success(self, table_name, max_timestamp, cursor=None):
        """
        Update checkpoint after successful load.

        If a cursor is provided, reuse it so the checkpoint
        update can participate in an existing transaction.
        """

        max_dttm_str = (
            f"'{max_timestamp}'"
            if max_timestamp
            else "'1970-01-01 00:00:00'::TIMESTAMP"
        )

        query = f"""
            UPDATE {self.database}.{self.schema}.control_table
            SET max_etl_update_dttm = {max_dttm_str},
                last_sync_dttm =
                CONVERT_TIMEZONE('America/Los_Angeles','America/New_York',CURRENT_TIMESTAMP),
                last_sync_status = 'PASS',
                comment = 'Data load successful'
            WHERE table_name = '{table_name}'
        """


        if cursor is not None:
            cursor.execute(query)

            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Checkpoint update failed for {table_name}: "
                    f"expected 1 updated row, got {cursor.rowcount}"
                )
        else:
            with self.snowflake.connection.cursor() as new_cursor:
                new_cursor.execute(query)

                if new_cursor.rowcount != 1:
                    raise RuntimeError(
                        f"Checkpoint update failed for {table_name}: "
                        f"expected 1 updated row, got {new_cursor.rowcount}"
                    )

import logging
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from lib.get_primary_keys import get_primary_keys
from lib.clean_prev_temp_tables import clean_prev_instance_temp_tables

logger = logging.getLogger(__name__)


class TableLoader:

    def __init__(
        self,
        postgres,
        snowflake,
        control_table,
        tables,
        pg_schema,
        sf_database,
        sf_schema
    ):
        self.postgres = postgres
        self.snowflake = snowflake
        self.control_table = control_table
        self.tables = tables
        self.pg_schema = pg_schema
        self.sf_database = sf_database
        self.sf_schema = sf_schema

    def load_tables(self):

        logger.info("Starting table load process")

        pk_keys = get_primary_keys(self.postgres.pg_uri, self.pg_schema)

        with self.snowflake.connection.cursor() as cursor:
            clean_prev_instance_temp_tables(
                cursor,
                self.sf_database,
                self.sf_schema
            )

        for table in self.tables:

            try:

                self._load_single_table(table, pk_keys)

            except Exception as e:

                logger.error("Failed loading table %s: %s", table, e)

                self.control_table.mark_failure(
                    table,
                    "Data load error"
                )

    def _load_single_table(self, table, pk_keys):

        logger.info("Processing table: %s", table)

        last_load_time = self.control_table.get_last_load_time(table)

        query = f"""
            SELECT *
            FROM {self.pg_schema}.{table}
            WHERE etl_update_dttm > CAST('{last_load_time}' AS timestamp)
        """

        df = pd.read_sql_query(query, self.postgres.connection)

        if df.empty:

            logger.info("No new records for %s", table)

            self.control_table.mark_no_data(table)

            return

        logger.info("Fetched %s rows from Postgres", len(df))

        df = self._transform_dataframe(df)

        temp_table = f"temp_{table}"

        self._create_temp_table(table, temp_table)

        self._write_dataframe(df, temp_table)

        self._merge_into_target(table, temp_table, pk_keys)

        max_dttm = df["etl_update_dttm"].max()

        self.control_table.mark_success(table, max_dttm)

        logger.info("Finished processing table: %s", table)

    def _transform_dataframe(self, df):

        for col in df.columns:

            if "_dttm" in col:

                df[col] = (
                    pd.to_datetime(df[col], errors="coerce")
                    .dt.tz_localize("UTC")
                    .dt.tz_convert("America/New_York")
                    .dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                )

        return df

    def _create_temp_table(self, table, temp_table):

        query = f"""
            CREATE TABLE {self.sf_database}.{self.sf_schema}.{temp_table}
            LIKE {self.sf_database}.{self.sf_schema}.{table}
        """

        with self.snowflake.connection.cursor() as cursor:

            cursor.execute(query)

        logger.info("Created temp table: %s", temp_table)

    def _write_dataframe(self, df, temp_table):

        success, chunks, rows, _ = write_pandas(
            self.snowflake.connection,
            df,
            temp_table,
            auto_create_table=False,
            quote_identifiers=False,
            on_error="CONTINUE"
        )

        if not success:

            raise RuntimeError("Snowflake load failed")

        logger.info("Loaded %s rows into %s", rows, temp_table)

    def _merge_into_target(self, table, temp_table, pk_keys):

        pk_col = pk_keys.get(table)[0]

        delete_query = f"""
            DELETE FROM {self.sf_database}.{self.sf_schema}.{table}
            WHERE {pk_col} IN (
                SELECT {pk_col}
                FROM {self.sf_database}.{self.sf_schema}.{temp_table}
            )
        """

        insert_query = f"""
            INSERT INTO {self.sf_database}.{self.sf_schema}.{table}
            SELECT *
            FROM {self.sf_database}.{self.sf_schema}.{temp_table}
        """

        drop_query = f"""
            DROP TABLE IF EXISTS
            {self.sf_database}.{self.sf_schema}.{temp_table}
        """

        with self.snowflake.connection.cursor() as cursor:

            cursor.execute(delete_query)
            cursor.execute(insert_query)
            cursor.execute(drop_query)

        logger.info("Merged data into %s", table)
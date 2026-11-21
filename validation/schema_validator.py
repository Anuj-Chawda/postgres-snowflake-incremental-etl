import logging
import sys

from lib.compare_schema import schema_comparison

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validates schema compatibility between PostgreSQL and Snowflake
    before starting the data sync process.
    """

    def __init__(
        self,
        postgres_connector,
        snowflake_connector,
        tables,
        pg_schema,
        sf_database,
        sf_schema
    ):
        self.postgres = postgres_connector
        self.snowflake = snowflake_connector
        self.tables = tables
        self.pg_schema = pg_schema
        self.sf_database = sf_database
        self.sf_schema = sf_schema

    def validate(self):
        """
        Perform schema validation between PostgreSQL and Snowflake.
        """

        logger.info("Starting schema validation.")

        schema_results = schema_comparison(
            self.postgres.pg_uri,
            self.snowflake.connection,
            self.pg_schema,
            self.sf_database,
            self.sf_schema,
            self.tables
        )

        missing_tables = schema_results["missing_tables_in_snowflake"]
        tables_compared = schema_results["tables_compared"]

        schema_issues_found = False

        # Missing tables
        for table_name in missing_tables:

            logger.error("Table '%s' is missing in Snowflake.", table_name)
            schema_issues_found = True

            with self.snowflake.connection.cursor() as cursor:
                update_control_query = f"""
                    UPDATE {self.sf_database}.{self.sf_schema}.control_table
                    SET last_sync_dttm = CONVERT_TIMEZONE('America/Los_Angeles','America/New_York',CURRENT_TIMESTAMP),
                        last_sync_status = 'FAIL',
                        comment = 'Table Missing in Snowflake'
                    WHERE table_name = '{table_name}'
                """
                cursor.execute(update_control_query)

        # Column mismatches
        for table_name, details in tables_compared.items():

            missing_columns = details["missing_columns"]
            datatype_mismatches = details["datatype_mismatches"]

            if missing_columns or datatype_mismatches:

                logger.error("Schema mismatch detected for table '%s'", table_name)
                schema_issues_found = True

                if missing_columns:
                    logger.error("Missing columns: %s", missing_columns)

                if datatype_mismatches:
                    logger.error("Datatype mismatches: %s", datatype_mismatches)

                with self.snowflake.connection.cursor() as cursor:
                    update_control_query = f"""
                        UPDATE {self.sf_database}.{self.sf_schema}.control_table
                        SET last_sync_dttm = CONVERT_TIMEZONE('America/Los_Angeles','America/New_York',CURRENT_TIMESTAMP),
                            last_sync_status = 'FAIL',
                            comment = 'Schema Mismatch'
                        WHERE table_name = '{table_name}'
                    """
                    cursor.execute(update_control_query)

        if schema_issues_found:
            logger.error("Schema validation failed. Stopping pipeline.")
            sys.exit(1)

        logger.info("Schema validation completed successfully.")
import logging

from validation.schema_validator import SchemaValidator
from metadata.control_table import ControlTable
from loaders.table_loader import TableLoader


logger = logging.getLogger(__name__)


class SyncPipeline:

    def __init__(
        self,
        postgres,
        snowflake,
        tables,
        pg_schema,
        sf_database,
        sf_schema
    ):
        self.postgres = postgres
        self.snowflake = snowflake
        self.tables = tables
        self.pg_schema = pg_schema
        self.sf_database = sf_database
        self.sf_schema = sf_schema

    def run(self):

        logger.info("Starting Sync Pipeline")

        validator = SchemaValidator(
            self.postgres,
            self.snowflake,
            self.tables,
            self.pg_schema,
            self.sf_database,
            self.sf_schema
        )

        validator.validate()

        control_table = ControlTable(
            self.snowflake,
            self.sf_database,
            self.sf_schema
        )

        loader = TableLoader(
            self.postgres,
            self.snowflake,
            control_table,
            self.tables,
            self.pg_schema,
            self.sf_database,
            self.sf_schema
        )

        loader.load_tables()

        logger.info("Sync Pipeline Completed")
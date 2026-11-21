import logging
from typing import Optional, Any, List

import snowflake.connector


logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """
    Connector class responsible for managing Snowflake connections
    and executing queries.
    """

    def __init__(self, config: dict, private_key: bytes):
        self.config = config
        self.private_key = private_key
        self.connection: Optional[Any] = None

    def connect(self) -> None:
        """Establish a Snowflake connection."""
        try:
            self.connection = snowflake.connector.connect(
                account=self.config["sf_account"],
                user=self.config["sf_user"],
                role=self.config["sf_role"],
                private_key=self.private_key,
                warehouse=self.config["sf_warehouse"],
                database=self.config["sf_database"],
                schema=self.config["sf_schema"],
            )

            logger.info("Snowflake connection established.")

        except Exception as e:
            logger.error("Failed to connect to Snowflake: %s", e)
            raise

    def test_connection(self) -> bool:
        """Test Snowflake connectivity."""
        try:
            if not self.connection:
                self.connect()

            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()

            logger.info("Snowflake connection test successful.")
            return True

        except Exception as e:
            logger.error("Snowflake connection test failed: %s", e)
            return False

    def execute(self, query: str) -> None:
        """Execute a SQL statement."""
        if not self.connection:
            raise RuntimeError("Snowflake connection is not established.")

        cursor = self.connection.cursor()

        try:
            cursor.execute(query)

        finally:
            cursor.close()

    def fetch_all(self, query: str) -> List[Any]:
        """Execute query and return all rows."""
        if not self.connection:
            raise RuntimeError("Snowflake connection is not established.")

        cursor = self.connection.cursor()

        try:
            cursor.execute(query)
            return cursor.fetchall()

        finally:
            cursor.close()

    def close(self) -> None:
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("Snowflake connection closed.")
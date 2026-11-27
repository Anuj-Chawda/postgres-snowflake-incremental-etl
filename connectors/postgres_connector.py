import logging
from typing import Optional, Any, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection


logger = logging.getLogger(__name__)


class PostgresConnector:
    """
    Connector class responsible for managing PostgreSQL connections
    and executing queries.
    """

    def __init__(self, pg_uri: str):
        self.pg_uri = pg_uri
        self.engine: Optional[Engine] = None
        self.connection: Optional[Connection] = None

    def connect(self) -> None:
        """Create engine and open connection."""
        try:
            self.engine = create_engine(self.pg_uri)
            self.connection = self.engine.connect()

            logger.info("PostgreSQL connection established.")

        except Exception as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    def reconnect(self) -> None:
        """
        Replace the existing PostgreSQL connection after a failure.

        Closing the old connection ends its transaction.
        A fresh connection is then obtained from the engine.
        """
        logger.warning("Reconnecting to PostgreSQL.")

        if self.engine is None:
            self.connect()
            return

        if self.connection is not None:
            self.connection.close()
            self.connection = None

        try:
            self.connection = self.engine.connect()
            logger.info("PostgreSQL connection re-established.")

        except Exception:
            logger.exception("Failed to reconnect to PostgreSQL.")
            raise

    def test_connection(self) -> bool:
        """Test PostgreSQL connectivity."""
        try:
            if not self.connection:
                self.connect()

            self.connection.execute(text("SELECT 1"))

            logger.info("PostgreSQL connection test successful.")
            return True

        except Exception as e:
            logger.error("PostgreSQL connection test failed: %s", e)
            return False

    def execute(self, query: str) -> None:
        """Execute a SQL query."""
        if not self.connection:
            raise RuntimeError("PostgreSQL connection not established.")

        self.connection.execute(text(query))

    def fetch_all(self, query: str) -> List[Any]:
        """Execute query and return all rows."""
        if not self.connection:
            raise RuntimeError("PostgreSQL connection not established.")

        result = self.connection.execute(text(query))

        return result.fetchall()

    def close(self) -> None:
        """Close connection and dispose engine."""
        if self.connection:
            self.connection.close()

        if self.engine:
            self.engine.dispose()

        logger.info("PostgreSQL connection closed.")
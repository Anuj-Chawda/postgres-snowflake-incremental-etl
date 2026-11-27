
from unittest.mock import MagicMock

from connectors.postgres_connector import PostgresConnector


def test_reconnect_replaces_existing_connection():
    """
    Verify that reconnect() closes the existing connection
    and replaces it with a fresh SQLAlchemy connection.
    """

    connector = PostgresConnector(
        "postgresql://test_user:test_password@localhost/test_db"
    )

    # Simulate an existing SQLAlchemy engine and connection.
    old_connection = MagicMock()
    new_connection = MagicMock()
    engine = MagicMock()

    engine.connect.return_value = new_connection

    connector.engine = engine
    connector.connection = old_connection

    # Execute the real reconnect() method.
    connector.reconnect()

    # Verify that the old connection was closed.
    old_connection.close.assert_called_once()

    # Verify that a new connection was requested.
    engine.connect.assert_called_once_with()

    # Verify that the connector now uses the new connection.
    assert connector.connection is new_connection

    # Verify that the existing engine was reused.
    assert connector.engine is engine

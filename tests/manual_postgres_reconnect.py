
from pathlib import Path
from urllib.parse import quote

from sqlalchemy import text

from config.config_loader import ConfigLoader
from connectors.postgres_connector import PostgresConnector
from lib.decryption import decrypt_data


def main():
    project_root = Path(__file__).resolve().parent.parent

    config = ConfigLoader.load(
        str(project_root / "config.yaml")
    )

    password = decrypt_data(config["pg_password"])

    if password is None:
        raise RuntimeError("Could not decrypt PostgreSQL password")

    pg_uri = (
        f"postgresql://{quote(config['pg_user'], safe='')}:"
        f"{quote(password, safe='')}@"
        f"{config['pg_host']}:{config['pg_port']}/"
        f"{config['pg_database']}"
    )

    postgres = PostgresConnector(pg_uri)

    try:
        # Test the initial connection.
        postgres.connect()

        first_connection = postgres.connection

        result = postgres.connection.execute(
            text("SELECT 1")
        ).scalar_one()

        assert result == 1

        print("PASS: Initial PostgreSQL connection")

        # Test reconnection using the existing engine.
        postgres.reconnect()

        assert postgres.connection is not first_connection

        print("PASS: Connection replaced successfully")

        # Verify the replacement connection works.
        result = postgres.connection.execute(
            text("SELECT 1")
        ).scalar_one()

        assert result == 1

        print("PASS: PostgreSQL query after reconnection")

    finally:
        postgres.close()

    print("PASS: PostgreSQL reconnection integration test")


if __name__ == "__main__":
    main()

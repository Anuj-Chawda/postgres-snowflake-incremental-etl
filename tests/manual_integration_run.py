
import ast
from pathlib import Path
from urllib.parse import quote

from cryptography.hazmat.primitives import serialization

from config.config_loader import ConfigLoader
from connectors.postgres_connector import PostgresConnector
from connectors.snowflake_connector import SnowflakeConnector
from lib.decryption import decrypt_data
from pipeline.sync_pipeline import SyncPipeline


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main():

    config_path = PROJECT_ROOT / "config.integration.yaml"

    if not config_path.is_file():
        raise FileNotFoundError(
            "Integration configuration not found."
        )

    config = ConfigLoader.load(str(config_path))
    tables = ast.literal_eval(config["tables"])

    # Safety checks: never use the existing iceDQ source.
    if (
        config["pg_host"].lower() not in ("localhost", "127.0.0.1")
        or config["pg_database"].lower() != "etl_test_db"
        or config["pg_schema"].lower() != "public"
        or config["sf_database"].upper() != "ICEDQ_DB"
        or config["sf_schema"].upper() != "ETL_INTEGRATION_TEST"
        or tables != ["etl_integration_test"]
    ):
        raise RuntimeError(
            "Integration configuration safety check failed."
        )

    print("PASS: Integration environment safety checks")

    # Decrypt PostgreSQL credentials.
    pg_password = decrypt_data(config["pg_password"])

    if pg_password is None:
        raise RuntimeError(
            "Failed to decrypt PostgreSQL password."
        )

    pg_uri = (
        f"postgresql://{quote(config['pg_user'], safe='')}:"
        f"{quote(pg_password, safe='')}@"
        f"{config['pg_host']}:{config['pg_port']}/"
        f"{config['pg_database']}"
    )

    # Decrypt Snowflake private-key passphrase.
    passphrase = decrypt_data(
        config["snow_key_pair_passphrase"]
    )

    if passphrase is None:
        raise RuntimeError(
            "Failed to decrypt Snowflake key passphrase."
        )

    with open(PROJECT_ROOT / "rsa_key.p8", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode(),
        )

    postgres = PostgresConnector(pg_uri)
    snowflake = SnowflakeConnector(config, private_key)

    try:
        postgres.connect()
        snowflake.connect()

        print("PASS: Database connections established")

        pipeline = SyncPipeline(
            postgres,
            snowflake,
            tables,
            config["pg_schema"],
            config["sf_database"],
            config["sf_schema"],
        )

        print("Starting integration ETL test")

        pipeline.run()

        print("Integration pipeline execution returned")

    finally:
        postgres.close()
        snowflake.close()


if __name__ == "__main__":
    main()

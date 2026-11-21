import os
import logging
import datetime
import ast
from cryptography.hazmat.primitives import serialization
from urllib.parse import quote

# Import custom library functions
from lib.delete_old_logs import delete_old_log_files
from lib.decryption import decrypt_data

# Get Current date and time
logsTime = datetime.datetime.now()

# Create a directory to store log files if it doesn't exist
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Delete old log files before starting
delete_old_log_files(log_directory)

# Generate a unique log file name using a timestamp
log_file = os.path.join(log_directory, f"LogFile_{logsTime.strftime('%Y%m%d')}.log")

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt='%d-%b-%Y %H:%M:%S')

# Create a handler to log to the file
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt='%d-%b-%Y %H:%M:%S'))
logging.getLogger().addHandler(file_handler)

from config.config_loader import ConfigLoader
from connectors.postgres_connector import PostgresConnector
from connectors.snowflake_connector import SnowflakeConnector
from pipeline.sync_pipeline import SyncPipeline

def main():

    logging.info("Starting new ETL Instance")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "config.yaml")

    config = ConfigLoader.load(config_path)

    # decrypt credentials
    str_passphrase = decrypt_data(config['snow_key_pair_passphrase'])

    if str_passphrase is None:
        raise ValueError(
            "Failed to decrypt Snowflake private key passphrase. "
            "Verify encryption_key.key and the encrypted configuration."
        )

    passphrase = str_passphrase.encode()

    p8_key_path = os.path.join(os.getcwd(), "rsa_key.p8")

    with open(p8_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase
        )

    pg_password = decrypt_data(config['pg_password'])

    if pg_password is None:
        raise ValueError(
            "Failed to decrypt PostgreSQL password. "
            "Verify encryption_key.key and the encrypted configuration."
        )

    pg_uri = (
        f"postgresql://{quote(config['pg_user'], safe='')}:"
        f"{quote(pg_password, safe='')}@"
        f"{config['pg_host']}:{config['pg_port']}/"
        f"{config['pg_database']}"
    )

    # connectors
    postgres = PostgresConnector(pg_uri)

    snowflake = SnowflakeConnector(config, private_key)

    # test connections
    if not postgres.test_connection():
        logging.error("Unable to connect to PostgreSQL")
        exit(1)

    if not snowflake.test_connection():
        logging.error("Unable to connect to Snowflake")
        exit(1)

    postgres.connect()
    snowflake.connect()

    logging.info("All connections established.")

    tables = ast.literal_eval(config["tables"])

    pipeline = SyncPipeline(
        postgres,
        snowflake,
        tables,
        config["pg_schema"],
        config["sf_database"],
        config["sf_schema"]
    )

    pipeline.run()

    postgres.close()
    snowflake.close()

    logging.info("ETL Instance Completed")

if __name__ == "__main__":
    main()

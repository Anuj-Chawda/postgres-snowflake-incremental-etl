
import argparse
import getpass
from pathlib import Path

import yaml
from cryptography.fernet import Fernet, InvalidToken


KEY_PATH = Path("encryption_key.key")

CREDENTIAL_FIELDS = [
    "snow_key_pair_passphrase",
    "pg_password",
]

def get_config_path():
    """Get the YAML configuration path from command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Encrypt credentials in a YAML configuration file."
    )

    parser.add_argument(
        "config_path",
        nargs="?",
        default="config.yaml",
        help="Path to the YAML configuration file (default: config.yaml)"
    )

    args = parser.parse_args()
    return Path(args.config_path)

def load_or_create_key():
    """Load the existing Fernet key or generate one if necessary."""

    if KEY_PATH.exists():
        print("Using existing encryption key.")
        return KEY_PATH.read_bytes()

    key = Fernet.generate_key()

    with KEY_PATH.open("xb") as key_file:
        key_file.write(key)

    print("Generated a new encryption key.")
    return key


def is_already_encrypted(value, cipher):
    """Check whether a value can be decrypted using the current key."""

    if not isinstance(value, str):
        return False

    try:
        cipher.decrypt(value.encode())
        return True

    except (InvalidToken, ValueError):
        if value.startswith("gAAAA"):
            raise ValueError(
                "An existing Fernet token could not be decrypted. "
                "Check that encryption_key.key is the correct key. "
                "No configuration changes have been written."
            )

        return False


def replace_values(obj, cipher, credentials, updated_fields, found_fields):
    """Recursively encrypt only credential fields that need encryption."""

    if isinstance(obj, dict):

        for key, value in obj.items():

            if key in CREDENTIAL_FIELDS:

                found_fields.add(key)

                if is_already_encrypted(value, cipher):
                    print(f"'{key}' is already encrypted. Skipping.")
                    continue

                # Prompt only once for each credential field.
                if key not in credentials:

                    credentials[key] = getpass.getpass(
                        f"Enter {key}: "
                    )

                    if not credentials[key]:
                        raise ValueError(
                            f"Empty value supplied for '{key}'. "
                            "Configuration was not updated."
                        )

                obj[key] = cipher.encrypt(
                    credentials[key].encode()
                ).decode()

                updated_fields.add(key)

                print(f"Encrypted '{key}'.")

            else:
                replace_values(
                    value,
                    cipher,
                    credentials,
                    updated_fields,
                    found_fields
                )

    elif isinstance(obj, list):

        for item in obj:
            replace_values(
                item,
                cipher,
                credentials,
                updated_fields,
                found_fields
            )


def main():

    config_path = get_config_path()

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_PATH}"
        )

    with config_path.open("r", encoding="utf-8") as yaml_file:
        config = yaml.safe_load(yaml_file) or {}

    key = load_or_create_key()
    cipher = Fernet(key)

    credentials = {}
    updated_fields = set()
    found_fields = set()

    replace_values(
        config,
        cipher,
        credentials,
        updated_fields,
        found_fields
    )

    missing_fields = set(CREDENTIAL_FIELDS) - found_fields

    if missing_fields:
        print(
            "Credential fields not found in configuration:",
            ", ".join(sorted(missing_fields))
        )

    if not updated_fields:
        print("All existing credential values are already encrypted.")
        print("No configuration changes were necessary.")
        return

    with config_path.open("w", encoding="utf-8") as yaml_file:
        yaml.safe_dump(
            config,
            yaml_file,
            default_flow_style=False
        )

    print(
        f"Configuration updated. "
        f"{len(updated_fields)} credential field(s) encrypted."
    )


if __name__ == "__main__":
    main()

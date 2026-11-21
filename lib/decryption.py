from pathlib import Path

from cryptography.fernet import Fernet


KEY_PATH = Path(__file__).resolve().parent.parent / "encryption_key.key"


def decrypt_data(encrypted_data):
    """
    Decrypt a Fernet-encrypted string using the project's encryption key.

    Returns:
        str: Decrypted plaintext on success.
        None: If decryption fails.
    """

    try:
        with KEY_PATH.open("rb") as key_file:
            key = key_file.read()

        cipher_suite = Fernet(key)

        decrypted_data = cipher_suite.decrypt(
            encrypted_data.encode()
        ).decode()

        return decrypted_data

    except Exception as e:
        print(f"Error during decryption: {e}")
        return None
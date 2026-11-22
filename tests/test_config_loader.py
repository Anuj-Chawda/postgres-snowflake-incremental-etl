
import pytest
import yaml

from config.config_loader import ConfigLoader


def test_load_valid_yaml(tmp_path):
    """Verify that a valid YAML configuration is loaded correctly."""

    config_file = tmp_path / "config.yaml"

    expected_config = {
        "pg_host": "localhost",
        "pg_port": 5432,
        "pg_database": "test_db",
        "tables": "['customers', 'orders']",
    }

    config_file.write_text(
        yaml.safe_dump(expected_config),
        encoding="utf-8",
    )

    result = ConfigLoader.load(str(config_file))

    assert result == expected_config
    assert result["pg_port"] == 5432


def test_load_missing_file(tmp_path):
    """Verify that a missing configuration file raises an error."""

    missing_file = tmp_path / "missing.yaml"

    with pytest.raises(FileNotFoundError):
        ConfigLoader.load(str(missing_file))


def test_load_invalid_yaml(tmp_path):
    """Verify that malformed YAML raises a parsing exception."""

    config_file = tmp_path / "invalid.yaml"

    config_file.write_text(
        "pg_host: [localhost",
        encoding="utf-8",
    )

    with pytest.raises(yaml.YAMLError):
        ConfigLoader.load(str(config_file))

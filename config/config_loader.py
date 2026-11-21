import yaml
import logging
from pathlib import Path
from typing import Dict, Any


logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Responsible for loading and validating configuration files.
    """

    @staticmethod
    def load(file_path: str) -> Dict[str, Any]:
        """
        Load YAML configuration file.

        Args:
            file_path: Path to config.yaml

        Returns:
            Dictionary containing configuration values.
        """

        path = Path(file_path)

        if not path.exists():
            logger.error("Config file not found: %s", file_path)
            raise FileNotFoundError(f"Config file not found: {file_path}")

        try:
            with path.open("r") as f:
                config = yaml.safe_load(f)

            logger.info("Configuration loaded successfully from %s", file_path)

            return config

        except yaml.YAMLError as e:
            logger.error("Invalid YAML format in config file")
            raise e
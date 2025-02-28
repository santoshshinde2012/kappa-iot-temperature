import yaml
import os
from pathlib import Path

class Config:
    _instance = None
    BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Points to kappa-iot-temperature/

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_configs()
        return cls._instance

    def _load_configs(self):
        self.kafka = self._load_file(self.BASE_DIR / "config" / "kafka_config.yaml")
        self.spark = self._load_file(self.BASE_DIR / "config" / "spark_config.yaml")
        self.cassandra = self._load_file(self.BASE_DIR / "config" / "cassandra_config.yaml")

    def _load_file(self, path):
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with path.open('r') as file:
            return yaml.safe_load(file)

    @property
    def kafka_config(self):
        return self.kafka["kafka"]

    @property
    def spark_config(self):
        return self.spark["spark"]

    @property
    def cassandra_config(self):
        return self.cassandra["cassandra"]

    def get_logging(self, config_type="kafka"):
        return self.__dict__[config_type]["logging"]

config = Config()

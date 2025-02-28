from cassandra.cluster import Cluster
from src.utils.config import config
from src.utils.logger import get_logger


class CassandraRepository:
    def __init__(self):
        self.logger = get_logger(
            "cassandra_repository", **config.get_logging("cassandra")
        )
        self.cassandra_config = config.cassandra_config
        self.cluster = None  # Initialize to None to avoid AttributeError in __del__
        try:
            self.cluster = Cluster(
                [self.cassandra_config["contact_points"]],
                port=self.cassandra_config["port"],
            )
            self.session = self.cluster.connect()
            self._setup_schema()
        except Exception as e:
            self.logger.error(f"Failed to connect to Cassandra: {e}")
            raise

    def _setup_schema(self):
        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {self.cassandra_config['keyspace']} "
            f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        )
        self.session.set_keyspace(self.cassandra_config["keyspace"])
        self.session.execute(
            f"CREATE TABLE IF NOT EXISTS {self.cassandra_config['table']} ("
            f"sensor_id text, timestamp double, temperature double, is_anomaly boolean, "
            f"PRIMARY KEY (sensor_id, timestamp))"
        )
        self.logger.info(
            f"Schema setup complete for {self.cassandra_config['keyspace']}.{self.cassandra_config['table']}"
        )

    def find_anomalies(self):
        rows = self.session.execute(
            f"SELECT * FROM {self.cassandra_config['table']} WHERE is_anomaly = true ALLOW FILTERING"
        )
        for row in rows:
            self.logger.info(
                f"Anomaly: sensor_id={row.sensor_id}, temp={row.temperature}, time={row.timestamp}"
            )
        return list(rows)

    def fetch_all_data(self):
        rows = self.session.execute(f"SELECT * FROM {self.cassandra_config['table']}")
        return pd.DataFrame(
            [
                {
                    "sensor_id": row.sensor_id,
                    "timestamp": row.timestamp,
                    "temperature": row.temperature,
                    "is_anomaly": row.is_anomaly,
                }
                for row in rows
            ]
        )

    def __del__(self):
        if self.cluster is not None:  # Check if cluster was initialized
            self.cluster.shutdown()


if __name__ == "__main__":
    repo = CassandraRepository()
    repo.find_anomalies()

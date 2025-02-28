from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
import pickle
import os
from pathlib import Path

# Load the pre-trained model globally
MODEL_PATH = Path(__file__).resolve().parent.parent.parent / "models/anomaly_detector.pkl"
if not MODEL_PATH.exists():
    raise FileNotFoundError(f"Model file not found: {MODEL_PATH}. Run train_anomaly_detector.py first.")
with open(MODEL_PATH, "rb") as f:
    DETECTOR = pickle.load(f)

def detect_anomaly(temp):
    """Standalone anomaly detection function for UDF."""
    try:
        return bool(DETECTOR.predict([[temp]])[0] == -1)
    except Exception as e:
        print(f"Anomaly detection error: {e}")
        return False

class SparkStreamProcessor:
    def __init__(self):
        self.spark = self._init_spark()
        self.topic = "temperature-data"
        self.keyspace = "iot_temperature"
        self.table = "temperature_results"

    def _init_spark(self):
        return SparkSession.builder \
            .master("local[*]") \
            .appName("TemperatureStreamProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
            .getOrCreate()

    def _get_schema(self):
        return StructType([
            StructField("sensor_id", StringType()),
            StructField("temperature", DoubleType()),
            StructField("timestamp", DoubleType())
        ])

    def _read_stream(self):
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:29092") \
            .option("subscribe", self.topic) \
            .load()

    def _process_stream(self, df):
        schema = self._get_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        anomaly_udf = self.spark.udf.register("detect_anomaly", detect_anomaly, BooleanType())
        return parsed_df.withColumn("is_anomaly", anomaly_udf(col("temperature")))

    def run(self):
        print("Starting Spark Streaming Locally")
        stream_df = self._read_stream()
        processed_df = self._process_stream(stream_df)
        query = processed_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.keyspace) \
            .option("table", self.table) \
            .outputMode("append") \
            .start()
        query.awaitTermination()

if __name__ == "__main__":
    SparkStreamProcessor().run()

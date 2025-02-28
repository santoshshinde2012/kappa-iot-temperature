from kafka import KafkaProducer
import json
import time
import random
from src.utils.config import config
from src.utils.logger import get_logger

class TemperatureProducer:
    def __init__(self):
        self.logger = get_logger("temperature_producer", **config.get_logging("kafka"))
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1
        )
        self.topic = config.kafka_config["topic"]

    def generate_temperature(self):
        temp = random.uniform(20.0, 40.0)
        if random.random() < 0.1:
            temp += random.uniform(10, 20)
        return temp

    def run(self):
        self.logger.info("Starting temperature producer")
        while True:
            try:
                data = {
                    "sensor_id": "sensor_1",
                    "temperature": self.generate_temperature(),
                    "timestamp": time.time()
                }
                self.producer.send(self.topic, value=data)
                self.logger.info(f"Sent: {data}")
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Failed to send data: {e}")
                time.sleep(5)

if __name__ == "__main__":
    TemperatureProducer().run()

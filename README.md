# Kappa IoT Temperature Monitoring

A Kappa Architecture implementation for real-time temperature monitoring using Apache Spark and Apache Cassandra, orchestrated with Docker Compose.

Real-time temperature monitoring with Kafka, Spark, and Cassandra.

## Components

1. Kafka: Streams temperature data via the temperature-data topic (auto-created).
2. Spark: Processes the stream locally, detecting anomalies with a pre-trained model.
3. Cassandra: Stores processed data in the iot_temperature.temperature_results table.
4. Streamlit: Displays real-time data and anomalies via a web dashboard.

## Project Structure

```
kappa-iot-temperature/
├── src/
│   ├── data_source/
│   │   ├── __init__.py
│   │   └── temperature_producer.py
│   ├── stream_processing/
│   │   ├── __init__.py
│   │   ├── train_anomaly_detector.py
│   │   └── spark_stream_processor.py
│   ├── serving/
│   │   ├── __init__.py
│   │   └── cassandra_repository.py
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       └── logger.py
├── config/
│   ├── kafka_config.yaml
│   ├── cassandra_config.yaml
├── models/
│   └── anomaly_detector.pkl
├── logs/
│   └── app.log
├── docker-compose.yml
├── main.py
├── dashboard.py
├── README.md
├── requirements.txt
└── .gitignore
```

## Key Files:

- `temperature_producer.py`: Simulates IoT sensor data and sends it to Kafka.
- `train_anomaly_detector.py`: Trains the anomaly detection model and saves it as anomaly_detector.pkl.
- `spark_stream_processor.py`: Processes the Kafka stream locally with Spark, detecting anomalies.
- `cassandra_repository.py`: Queries anomalies from Cassandra (used by main.py).
- `dashboard.py`: Streamlit dashboard for real-time visualization.

## Prerequisites

- Java 8+: Required for Spark (java -version).
- Spark 3.5.0: Installed locally (not Dockerized).
- Python 3.12: For compatibility (python3 --version).
- Docker: For Kafka and Cassandra services.
- Operating System: Tested on macOS (adjust paths for Windows if needed).

## Setup Instructions

#### Install Spark Locally

- Download Spark 3.5.0 with Hadoop 3 from Apache Spark [Downloads](https://spark.apache.org/downloads.html)
- Verify: `spark-submit --version`


#### Create Virtual Environment:

- Navigate to Project Root `cd /path/to/kappa-iot-temperature`
- Create Virtual Environment
  
  ```
  rm -rf venv
    python3 -m venv venv
    source venv/bin/activate
  ```

- Install Dependencies

    ```
    pip install -r requirements.txt
    ```

## Running Instructions

#### 1. Start Kafka and Cassandra Services

```   
docker compose up -d
```

- Verify Kafka UI: Check http://localhost:9090 for Kafka UI.
- Create Kafka Topic (with temperature-data).
- Verify Cassandra: http://localhost:3000/ for Cassandra UI.


#### 2. Train the Anomaly Detection Model (Terminal 1)

```
cd /path/to/kappa-iot-temperature
source venv/bin/activate
python src/stream_processing/train_anomaly_detector.py
```

- Details: Trains an Isolation Forest model and saves it to models/anomaly_detector.pkl.
- Output: Console prints "Training anomaly detector" and "Model saved to ...".
- Note: Run this once to generate the model file before processing; it exits after completion.

#### 3. Run the Temperature Producer (Terminal 2)

```
cd /path/to/kappa-iot-temperature
source venv/bin/activate
python src/data_source/temperature_producer.py
```

- Details: Simulates IoT sensor data and sends it to the Kafka topic temperature-data.
- Output: Logs to logs/app.log with "Starting temperature producer" and "Sent: ..." messages.
- Note: Keep this running to continuously generate data.

#### 4. Run Spark Streaming Processor (Terminal 3)

```
cd /path/to/kappa-iot-temperature
source venv/bin/activate
spark-submit --master "local[*]" --conf "spark.driver.extraJavaOptions=-Djava.security.manager=allow" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/stream_processing/spark_stream_processor.py
```

- Details: Processes the Kafka stream locally, applies the anomaly model, and writes results to Cassandra.
- Output: Console prints "Starting Spark Streaming Locally".
- Note: Keep this running to process the stream continuously.

#### 5. Run Streamlit Dashboard

```
cd /path/to/kappa-iot-temperature
source venv/bin/activate
streamlit run dashboard.py
```

- Details: Launches a web dashboard at http://localhost:8501 to visualize real-time data and anomalies.
- Output: Console prints "Connected to Cassandra" on successful connection.
- Note: Open http://localhost:8501 in your browser; keep this running for live updates.

#### 6. Stopping

- Stop Each Component: Press `Ctrl+C` in each terminal to stop the producer, Spark streaming, and dashboard.
- Stop Services:

`docker compose down --volumes`

- Details: Stops all Docker services and removes volumes.

<hr/>

### Connect with me on
<div id="badges">
  <a href="https://twitter.com/shindesan2012">
    <img src="https://img.shields.io/badge/shindesan2012-black?style=for-the-badge&logo=twitter&logoColor=white" alt="Twitter Badge"/>
  </a>
  <a href="https://www.linkedin.com/in/shindesantosh/">
    <img src="https://img.shields.io/badge/shindesantosh-blue?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn Badge"/>
  </a>
   <a href="https://blog.santoshshinde.com/">
    <img src="https://img.shields.io/badge/Blog-black?style=for-the-badge&logo=medium&logoColor=white" alt="Medium Badge"/>
  </a>
  <a href="https://www.buymeacoffee.com/santoshshin" target="_blank">
   <img src="https://img.shields.io/badge/buymeacoffee-black?style=for-the-badge&logo=buymeacoffee&logoColor=white" alt="Buy Me A Coffee"/>
  </a>
</div>

# Spark-HBase Integration

This project demonstrates how to connect a Spark job to an HBase instance running in a Docker container. The Spark job will ingest data from CSV files, apply transformations and load the result in Apache Hbase.

## Prerequisites

- Docker
- 16 GB of RAM
- Python 3.x

## Setup

1. Run the HBase Docker container:
    ```sh
    ./scripts/create_hbase_container.sh
    ```
2. Run the docker-compose.yml file:
    ```sh
    docker-compose up -d
    ```
3. Run the data_generation.py to generation the csv files:
    ```sh
    python data_generation.py
    ```
4. Run the Spark job:
    ```sh
    python spark_job.py
    ```

## File Structure

- `data/`: Contains the CSV data files.
- `scripts/`: Contains the shell script to start the HBase container and the script to set up airflow properly.
- `README.md`: Project documentation.
- `requirements.txt`: Python dependencies.

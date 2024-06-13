import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def get_kafka_producer(bootstrap_servers: list[str], retries: int = 5, delay: int = 30):
    for i in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers, max_block_ms=5000)
            return producer
        except NoBrokersAvailable as e:
            print(f'Attempt {i + 1}/{retries} to get Kafka broker failed: {e}')
            time.sleep(delay)
    raise Exception(f'Failed to connect to Kafka after {retries} retries')

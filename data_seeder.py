import time
from Kafka import Kafka
from data_faker import DataFaker



# Produce a few sample messages to the Kafka topic
for _ in range(10):
    sample_data = DataFaker.generate_random_data()
    Kafka.produce_message('customer-info-topic', sample_data)
    time.sleep(1)  # Add a delay to simulate real-time data

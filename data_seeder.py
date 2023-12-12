import time
from Kafka import Kafka
from data_faker import DataFaker
import json
import os


# Produce a few sample messages to the Kafka topic
for _ in range(10):
    sample_data = DataFaker.generate_random_data()
    Kafka.produce_message('customer-info-topic', sample_data)
    time.sleep(1)  # Add a delay to simulate real-time data

# Function to read data from a file and produce to Kafka
# def process_file(file_path, topic):
#     with open(file_path, 'r') as file:
#         try:
#             for line in file:
#                 Kafka.produce_message(topic, json.loads(line))
#         except json.JSONDecodeError as e:
#             print(f"Error decoding JSON from file {file_path}: {e}")

# data_folder = 'data'
# while True:
#     files = os.listdir(data_folder)
#     for file in files:
#         file_path = os.path.join(data_folder, file)
#         if os.path.isfile(file_path) and not file_path.endswith('.processed'):
#             process_file(file_path, 'customer-info-topic')
#             #mark file as processed
#             os.rename(file_path, file_path + '.processed')

#     time.sleep(1)
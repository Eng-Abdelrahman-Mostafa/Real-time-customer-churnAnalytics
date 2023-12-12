import time
from data_faker import DataFaker
import json


# Produce a few sample messages to the Kafka topic and write to files
for i in range(10):
    sample_data = DataFaker.generate_random_data()
    file_path = f'data/file_{i}.txt'
    with open(file_path, 'w') as file:
        json.dump(sample_data, file)  # Convert dictionary to a JSON string
      # Add a delay to simulate real-time data production

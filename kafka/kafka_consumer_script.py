import os
import time

from kafka import KafkaConsumer
from yaml import safe_load


script_dir = os.path.dirname(os.path.abspath(__file__)) 
config_path = os.path.join(script_dir, "kafka_config.yml")

with open(config_path, 'r') as file:
    config = safe_load(file)

kafka_brokers = config.get('kafka_broker')['local_bootstrap_servers']
kafka_topic = "train_station_info_batch_data"


def run_consumer():
    consumer = KafkaConsumer(kafka_topic,
                             bootstrap_servers=kafka_brokers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    print("Waiting for messages...")
    for message in consumer:
        data = message.value.decode('utf-8')
        print(f"Received data: {data}")



if __name__ == "__main__":
    run_consumer()
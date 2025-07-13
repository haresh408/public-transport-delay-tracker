import os
import sys
import time
import json
import pandas as pd
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from yaml import safe_load
from  kafka import KafkaProducer,KafkaAdminClient
from  dataset_simulation.dataset_helper_methods import create_dirs_and_get_path



SEND_DELAY_SECONDS = 2




# Kafka Configuration

script_dir = os.path.dirname(os.path.abspath(__file__)) 
config_path = os.path.join(script_dir, "kafka_config.yml")

with open(config_path, 'r') as file:
    config = safe_load(file)


# Kafka Broker & Topics Initialization
kafka_broker = config.get('kafka_broker', {})
kafka_topics = [v['topic'] for v in config.values() if isinstance(v, dict) and 'topic' in v]


# Function to convert CSV filename to PascalCase
def csvname_to_camel_case(filename):
    name = ""
    if filename.endswith("_data.csv"):
        name = filename.replace('_data.csv', '')
    else:
        name = filename.replace('.csv', '')

    parts = name.split('_')

    return parts[0].lower() + ''.join(word.capitalize() for word in parts[1:])


# Topic Creation
def create_topic():
    admin = KafkaAdminClient(bootstrap_servers=kafka_broker["local_bootstrap_servers"])
    for kafka_topic in kafka_topics:
        topic = NewTopic(name=kafka_topic, num_partitions=3, replication_factor=1)
        try:
            admin.create_topics(new_topics=[topic], validate_only=False)
            print(f"[INFO] Topic '{topic.name}' created successfully.")
        except TopicAlreadyExistsError:
            print(f"[INFO] Topic '{topic}' already exists.")

    admin.close()


# Send Batch Data
def send_batch_data(topic_name="train_station_info_batch_data"):

    producer = KafkaProducer(bootstrap_servers=kafka_broker["local_bootstrap_servers"],max_request_size=5242880) 

    csv_file_names = ['station_info.csv', 'train_master_data.csv', 'train_schedule_time_data.csv']
    
    for i in range(3):
        try:
            csv_file = os.path.join(create_dirs_and_get_path(False),csv_file_names[i])
            df = pd.read_csv(csv_file, encoding='latin-1')
            final_data = {
                "schema_type":csvname_to_camel_case(csv_file_names[i]),
                "data": df.to_json(orient='records')
            }

            producer.send(topic_name, value=json.dumps(final_data).encode('utf-8'))

            producer.flush()

            print(
                f"[INFO] Data from {csv_file_names[i]} sent to topic '{topic_name}' successfully "
                f"and size {((sys.getsizeof(json.dumps(final_data)))/(1024**2)):.4f} MB."
                )

        except FileNotFoundError:
            print(f"[WARN] File '{csv_file_names[i]}' not found. Retrying in 5s...")
            time.sleep(5)

        except Exception as e:
            print(f"[ERROR] Failed to send data from {csv_file_names[i]}: {e}")
            time.sleep(5)


def send_live_data(topic_name="train_live_status_stream_data"):
    producer = KafkaProducer(bootstrap_servers = kafka_broker["local_bootstrap_servers"])
    csv_file_name = 'train_live_status_data.csv'
    csv_file_path = os.path.join(create_dirs_and_get_path(False), csv_file_name)

    last_line = -1
    print(f"[INFO] Starting to send live data from {csv_file_name}...")

    while True:
        try:
            df = pd.read_csv(csv_file_path, skiprows=last_line+1, encoding='latin-1')
            if df.empty:
                print("[INFO] No new data to send. Retrying in 5 seconds...")
                time.sleep(5)
                continue

            for _,row in df.iterrows():
                producer.send(topic_name, value=json.dumps(row.to_dict()).encode('utf-8'))
                producer.flush()
                last_line += 1
                print(f'[INFO] Record sent:{last_line}')
                time.sleep(SEND_DELAY_SECONDS)

        except FileNotFoundError:
            print(f"[WARN] File '{csv_file_name}' not found. Retrying in 5s...")
            time.sleep(3)

        except Exception as e:
            print(f"[ERROR] Failed to send live data: {e}")
            time.sleep(3)


def run_producer():
    create_topic()
    send_batch_data()
    send_live_data()


if __name__ == "__main__":
    run_producer()



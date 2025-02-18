import time
import argparse
import random
from kafka_client import create_topic, send_message
from data_generator import generate_transaction
from dotenv import load_dotenv
import os

load_dotenv()

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
transaction_topic = 'transaction_data'
num_partitions = int(os.getenv('KAFKA_PARTITIONS', 2))
replication_factor = int(os.getenv('KAFKA_REPLICATION', 1))

create_topic(transaction_topic, num_partitions, replication_factor)

def produce_transactions(rate_tps, duration_seconds=60):
  
    interval = 1.0 / rate_tps
    end_time = time.time() + duration_seconds
    print(f"Producing at {rate_tps} TPS for {duration_seconds} seconds.")
    count = 0
    while time.time() < end_time:
        transaction = generate_transaction()
        ingestion_timestamp = time.time()
        transaction['ingestion_timestamp'] = ingestion_timestamp
        send_message(transaction_topic, transaction)
        count += 1
        time.sleep(interval)
    print(f"Finished producing ~{count} transactions.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce transactions at a given TPS.")
    parser.add_argument("--tps", type=int, default=10000, help="Transactions per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    args = parser.parse_args()

    produce_transactions(args.tps, args.duration)

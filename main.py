from concurrent.futures import ThreadPoolExecutor
import time
from kafka_client import create_topic, send_message
import random
from data_generator import (
    generate_customer,
    generate_product,
    generate_transaction,
    generate_product_view,
    generate_user_interaction,
    generate_system_log
)
# import threading
from dotenv import load_dotenv
import os

load_dotenv()
topic_name = os.getenv('KAFKA_TOPIC', 'stream-topic')
num_partitions = int(os.getenv('KAFKA_PARTITIONS', 2))
replication_factor = int(os.getenv('KAFKA_REPLICATION', 1))
customer_no = 1
product_no = 1

# Stream customer data
def stream_customer_data(topic):
    """
    Stream customer data to a Kafka topic.

    :param topic: str, name of the Kafka topic to which customer data is sent.
    """
    print(f"Starting customer data stream to topic '{topic}'...")
    try:
        customer = generate_customer()
        send_message(topic, customer)
    except KeyboardInterrupt:
        print("\nCustomer data streaming stopped.")

# Stream product data
def stream_product_data(topic):
    """
    Stream product data to a Kafka topic.

    :param topic: str, name of the Kafka topic to which product data is sent.
    """
    print(f"Starting product data stream to topic '{topic}'...")
    try:
        product = generate_product()
        send_message(topic, product)
    except KeyboardInterrupt:
        print("\nProduct data streaming stopped.")

# Stream transaction data
def stream_transaction_data(topic):
    """
    Stream transaction data to a Kafka topic.

    :param topic: str, name of the Kafka topic to which transaction data is sent.
    """
    print(f"Starting transaction data stream to topic '{topic}'...")
    try:
        transaction = generate_transaction()
        send_message(topic, transaction)
    except KeyboardInterrupt:
        print("\nTransaction data streaming stopped.")

# Stream product view data
def stream_product_view_data(topic):
    """
    Stream product view data to a Kafka topic.

    :param topic: str, name of the Kafka topic to which product view data is sent.
    """
    print(f"Starting product view data stream to topic '{topic}'...")
    try:
        product_view = generate_product_view()
        send_message(topic, product_view)
        if random.random() < 0.2:  # 20% chance to convert to transaction data
            transaction = generate_transaction(product_view["customer_id"], product_view["product_id"])
            send_message('transaction_data', transaction)
    except KeyboardInterrupt:
        print("\nProduct view data streaming stopped.")

# Stream user interaction data
def stream_user_interaction_data(topic):
    """
    Stream user interaction data to a Kafka topic.

    :param topic: str, name of the Kafka topic to which user interaction data is sent.
    """
    print(f"Starting user interaction data stream to topic '{topic}'...")
    try:
        user_interaction = generate_user_interaction()
        send_message(topic, user_interaction)
    except KeyboardInterrupt:
        print("\nUser interaction data streaming stopped.")

# Stream system log data
def stream_system_log_data(topic):
    """
    Stream system log data to a Kafka topic.

    :param topic: str, name of the Kafka topic to which system log data is sent.
    """
    print(f"Starting system log data stream to topic '{topic}'...")
    try:
        system_log = generate_system_log()
        send_message(topic, system_log)
    except KeyboardInterrupt:
        print("\nSystem log data streaming stopped.")

# Function to send data to Kafka
def send_data():
    # Occasionally add new customers or products
    """
    Send multiple types of data (customer, product, transaction, etc.) to Kafka.

    Handles the generation and sending of various data types to their respective Kafka topics.
    """
    global customer_no
    global product_no
    if customer_no < 500:
        stream_customer_data('customer_data')
        customer_no += 1
    else:
        if random.random() < 0.1:
            stream_customer_data('customer_data')
            customer_no += 1

    if product_no < 100:
        stream_product_data('product_data')
        product_no += 1
    else:
        if random.random() < 0.1:
            stream_product_data('product_data')
            product_no += 1
        
    # stream_transaction_data('transaction_data')
    stream_product_view_data('product_view_data')
    stream_user_interaction_data('user_interaction_data')

    stream_system_log_data('system_log_data')


if __name__ == "__main__":
    # Create the necessary topics
    """
    Main function to create Kafka topics and start data streaming.

    Initializes necessary Kafka topics and starts concurrent data streaming tasks using ThreadPoolExecutor.
    """
    create_topic('customer_data', num_partitions, replication_factor)
    create_topic('product_data', num_partitions, replication_factor)
    create_topic('transaction_data', num_partitions, replication_factor)
    create_topic('product_view_data', num_partitions, replication_factor)
    create_topic('user_interaction_data', num_partitions, replication_factor)
    create_topic('system_log_data', num_partitions, replication_factor)
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            executor.submit(send_data)
            time.sleep(random.uniform(1, 2))
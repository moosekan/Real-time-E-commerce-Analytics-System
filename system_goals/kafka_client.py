import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError


load_dotenv()

kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')


try:
    print(f"Connecting to Kafka broker at: {kafka_broker}")
    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("Kafka producer initialized.")
except KafkaError as e:
    print(f"Failed to initialize Kafka producer: {e}")
    raise

def create_topic(topic_name, num_partitions, replication_factor):

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Error creating topic '{topic_name}': {e}")
    finally:
        admin_client.close()

def send_message(topic, message):
    try:
        print(f"Connecting to Kafka broker at: {kafka_broker}")
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("Kafka producer initialized.")
    except KafkaError as e:
        print(f"Failed to initialize Kafka producer: {e}")
        raise
    try:
        producer.send(topic, value=message)
        producer.flush()
        print(f"Message sent to topic '{topic}': {message}")
    except KafkaError as e:
        print(f"Error sending message: {e}")
        raise

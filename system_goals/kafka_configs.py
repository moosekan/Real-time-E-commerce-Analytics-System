from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json
import time


def create_kafka_topic(kafka_brokers, topic_name, partitions, replication_factor):
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_brokers,
        client_id='topic_creator'
    )
    topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor
    )

    try: 
        admin_client.create_topics(new_topics=[topic], validate_only=False)
    except Exception as e:
        print(f"Error creating topic '{topic_name}': {e}")


def produce_to_kafka(kafka_brokers, kafka_topic, record_count):
    producer = KafkaProducer(
        bootstrap_servers=kafka_brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    start_time = time.time()

    for i in range(record_count):
        message = {
            "customer_id": f"cust_{i}",
            "name": f"Customer_{i}",
            "email": f"customer_{i}@example.com",
            "large_field": "X" * (32 * 32)
        }
        producer.send(kafka_topic, value=message, partition=i % 10)

    producer.flush()
    elapsed_time = time.time() - start_time
    print(f"Finished producing {record_count} records in {elapsed_time:.2f} seconds.")
    throughput = record_count / elapsed_time
    print(f"Producer Throughput: {throughput:.2f} records/second.")


if __name__ == "__main__":
    kafka_brokers = "localhost:9092"  
    kafka_topic = "customer_system_test_data" 
    partitions = 10                 
    replication_factor = 1            
    record_count = 200000   

    create_kafka_topic(kafka_brokers, kafka_topic, partitions, replication_factor)
    produce_to_kafka(kafka_brokers, kafka_topic, record_count)

import os
import json
import matplotlib.pyplot as plt
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from dotenv import load_dotenv
import time

load_dotenv()

kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092,localhost:9093,localhost:9094')

producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


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


def send_messages(topic, num_messages):
   
    try:
        start_time = time.time()
        for i in range(num_messages):
            message = {"message_number": i, "data": f"Message {i} for {topic}"}
            producer.send(topic, value=message)
        producer.flush()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Sent {num_messages} messages to topic '{topic}' in {elapsed_time:.2f} seconds.")
        return elapsed_time
    except KafkaError as e:
        print(f"Error sending messages to topic '{topic}': {e}")
        raise


def evaluate_configs(configs, message_counts):
   
    results = []

    for idx, config in enumerate(configs):
        for num_messages in message_counts:
            topic_name = f"test_system_config_{idx}_msgs_{num_messages}"
            print(f"\nEvaluating config: {config}, Messages: {num_messages}")
            create_topic(topic_name, config['num_partitions'], config['replication_factor'])
            elapsed_time = send_messages(topic_name, num_messages)

            results.append({
                "topic": topic_name,
                "num_partitions": config['num_partitions'],
                "replication_factor": config['replication_factor'],
                "num_messages": num_messages,
                "elapsed_time": elapsed_time
            })

    return results


def plot_results(results):

    message_counts = sorted(set(result['num_messages'] for result in results))
    replication_factors = sorted(set(result['replication_factor'] for result in results))

    for num_messages in message_counts:
        x = []
        y = []
        for rf in replication_factors:
            matching_results = [r for r in results if r['num_messages'] == num_messages and r['replication_factor'] == rf]
            if matching_results:
                x.append(rf)
                y.append(matching_results[0]['elapsed_time'])
        plt.plot(x, y, marker='o', label=f"{num_messages} Messages")

    plt.title("Replication Factor vs Elapsed Time")
    plt.xlabel("Replication Factor")
    plt.ylabel("Elapsed Time (seconds)")
    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    
    configs = [
        {"num_partitions": 3, "replication_factor": 1},
        {"num_partitions": 3, "replication_factor": 2},
        {"num_partitions": 3, "replication_factor": 3},
    ]

   
    message_counts = [1, 10, 100, 1000, 5000, 10000, 50000, 100000, 500000]

  
    results = evaluate_configs(configs, message_counts)

  
    plot_results(results)

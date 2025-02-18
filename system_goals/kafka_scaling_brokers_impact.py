from kafka import KafkaConsumer, TopicPartition
import threading
import time
import json


def consume_from_partition(kafka_brokers, kafka_topic, partition_id, record_count, results):
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_brokers,
        auto_offset_reset='latest',
        enable_auto_commit=False    
    )
    consumer.assign([TopicPartition(kafka_topic, partition_id)])
    start_time = time.time()
    messages_received = 0

    for message in consumer:
        messages_received += 1
        if messages_received >= record_count:
            break

    elapsed_time = time.time() - start_time
    throughput = messages_received / elapsed_time if elapsed_time > 0 else 0

    print(f"Consumer for partition {partition_id} finished.")
    print(f"Partition {partition_id} Throughput: {throughput:.2f} records/second.")

    results[partition_id] = {
        "partition_id": partition_id,
        "throughput": throughput,
        "messages_received": messages_received
    }

def run_consumer_group(kafka_brokers, kafka_topic, record_count):
    results = {}
    threads = []

    for partition_id in range(10):
        thread = threading.Thread(
            target=consume_from_partition,
            args=(kafka_brokers, kafka_topic, partition_id, record_count // 10, results)
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    total_throughput = sum([results[pid]["throughput"] for pid in results])
    return total_throughput


if __name__ == "__main__":
    
    kafka_brokers = "localhost:9092"  
    kafka_topic = "customer_system_test_data"  
    record_count = 12

    num_runs = 10
    total_throughput = 0

    for i in range(num_runs):
        print(f"\nRun {i + 1} of {num_runs}...")
        throughput = run_consumer_group(kafka_brokers, kafka_topic, record_count)
        total_throughput += throughput
        print(f"Run {i + 1} Throughput: {throughput:.2f} records/second")


    average_throughput = total_throughput / num_runs
    print(f"\n=== Average Throughput ===")
    print(f"Average Throughput over {num_runs} runs: {average_throughput:.2f} records/second")

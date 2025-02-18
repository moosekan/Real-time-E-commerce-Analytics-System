from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
from dotenv import load_dotenv
import time
import shutil  # For cleaning checkpoint directories

load_dotenv()

def process_stream_with_metrics(spark, kafka_bootstrap_servers, kafka_topic, record_limit, checkpoint_dir):

    userInteractionSchema = StructType([
        StructField("interaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("interaction_type", StringType(), True),
        StructField("details", StringType(), True)
    ])

    record_counter = spark.sparkContext.accumulator(0)

    start_time = time.time()

    def count_records(batch_df, batch_id):
        nonlocal record_counter, start_time
        batch_size = batch_df.count()

        # Update record counter
        record_counter += batch_size

        # Compute latency and throughput
        batch_time = time.time()
        elapsed_time = batch_time - start_time
        throughput = batch_size / elapsed_time if elapsed_time > 0 else 0
        latency = elapsed_time / record_counter.value if record_counter.value > 0 else 0

        print(f"Batch ID: {batch_id}")
        print(f"Batch Size: {batch_size}")
        print(f"Total Records Processed: {record_counter.value}")
        print(f"Elapsed Time: {elapsed_time:.2f} seconds")
        print(f"Throughput: {throughput:.2f} records/second")
        print(f"Latency: {latency:.8f} seconds/record")

        if record_counter.value >= record_limit:
            print(f"Reached record limit of {record_limit}. Stopping the stream.")
            spark.streams.active[0].stop()

    customerDF = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                  .option("subscribe", kafka_topic)
                  .option("startingOffsets", "latest")  # Start processing only new data
                  .load()
                  .selectExpr("CAST(value AS STRING)")
                  .select(from_json("value", userInteractionSchema).alias("data"))
                  .select("data.*"))

    query = customerDF.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_dir).foreachBatch(count_records) \
        .start()

    query.awaitTermination()


def evaluate_with_core_configs(kafka_bootstrap_servers, kafka_topic, record_limit, core_configs):
    results = []

    for i, (cores, memory) in enumerate(core_configs):
        print(f"Testing with {cores} cores and {memory} memory...")

        checkpoint_dir = f"/tmp/spark_checkpoint_config_{i}"
        
        # Clear checkpoint directory to restart processing
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)

        spark = SparkSession.builder \
            .appName("Ecommerce Data Pipeline - Core Config Evaluation") \
            .config("spark.executor.cores", cores) \
            .config("spark.executor.memory", memory) \
            .config("spark.es.nodes", "localhost") \
            .config("spark.es.port", "9200") \
            .config("spark.es.nodes.wan.only", "true") \
            .getOrCreate()

        start_time = time.time()

        try:
            process_stream_with_metrics(spark, kafka_bootstrap_servers, kafka_topic, record_limit, checkpoint_dir)
        except Exception as e:
            print(f"Error with {cores} cores and {memory}: {e}")
        finally:
            elapsed_time = time.time() - start_time
            results.append({
                "cores": cores,
                "memory": memory,
                "elapsed_time": elapsed_time
            })
            spark.stop()

    return results


if __name__ == "__main__":
    kafka_bootstrap_servers = os.getenv('KAFKA_BROKER', 'localhost:9092')
    kafka_topic = 'user_interaction_data'
    record_limit = 10

    core_memory_configs = [
        (1, "512m"),
        (2, "1g"),
        (4, "2g"),
        (8, "4g")
    ]

    performance_results = evaluate_with_core_configs(kafka_bootstrap_servers, kafka_topic, record_limit, core_memory_configs)

    print("\nPerformance Evaluation Results:")
    for result in performance_results:
        print(f"Cores: {result['cores']}, Memory: {result['memory']}, Elapsed Time: {result['elapsed_time']:.2f} seconds")

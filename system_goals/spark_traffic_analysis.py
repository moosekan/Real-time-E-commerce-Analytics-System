from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, unix_timestamp, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os

empty_batch_count = 0
max_empty_batches = 5 
query = None  


total_latency_sum = 0.0
total_message_count = 0

def handle_batch(batch_df, batch_id):
    global empty_batch_count
    global query
    global total_latency_sum
    global total_message_count

    if batch_df.rdd.isEmpty():
        empty_batch_count += 1
        if empty_batch_count >= max_empty_batches and query is not None: 
            query.stop()
    else:
      
        empty_batch_count = 0

        latency_df = batch_df.withColumn("processing_time_unix", unix_timestamp("processingTime")) \
                             .withColumn("latency_seconds", col("processing_time_unix") - col("ingestion_timestamp"))

        latency_stats = latency_df.agg(avg("latency_seconds").alias("avg_latency_seconds")).collect()
        avg_latency = latency_stats[0]["avg_latency_seconds"]

        record_count = batch_df.count()

        total_latency_sum += (avg_latency * record_count)
        total_message_count += record_count

        print(f"Average Latency in this batch: {avg_latency} seconds")

spark = SparkSession.builder \
    .appName("Performance across different loads") \
    .getOrCreate()

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic = "transaction_data"


transactionSchema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("ingestion_timestamp", DoubleType(), True)
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")  
    .load()
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json("json_str", transactionSchema).alias("data"))
    .select("data.*")
    .withColumn("processingTime", current_timestamp())
)

query = (
    df.writeStream
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .foreachBatch(handle_batch)
    .start()
)

query.awaitTermination()

if total_message_count > 0:
    overall_avg_latency = total_latency_sum / total_message_count
    print(f"Overall Average Latency across all processed messages: {overall_avg_latency} seconds")
else:
    print("No messages were processed during the run.")

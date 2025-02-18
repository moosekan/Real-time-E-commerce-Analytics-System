from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import udf 
from textblob import TextBlob
from pyspark.sql import DataFrame
from dotenv import load_dotenv
import os

load_dotenv()
import logging

# Set up logging configuration
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)



def analyze_sentiment(text):
    if text:
        analysis = TextBlob(text)
        if analysis.sentiment.polarity > 0:
            return "positive"
        elif analysis.sentiment.polarity < 0:
            return "negative"
        else:
            return "neutral"
    return "neutral"

# Register UDF
sentiment_udf = udf(analyze_sentiment, StringType())

# Initialize Spark Session with appropriate configurations for Ecommerce Data Analysis
spark = SparkSession.builder \
    .appName("Ecommerce Data Pipeline") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
            
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
# kafka_topic = os.getenv('KAFKA_TOPIC', '
# erce_customers')
# Read data from 'ecommerce_customers' topic
customerSchema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("account_created", StringType(), True),
    StructField("last_login", TimestampType(), True) 
])
customerDF = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
              .option("subscribe", 'customer_data')
              .option("startingOffsets", "earliest")  # Start from the earliest records
              .load()
              .selectExpr("CAST(value AS STRING)")
              .select(from_json("value", customerSchema).alias("data"))
              .select("data.*")
              .withColumn("processingTime", current_timestamp()) 
              .withWatermark("last_login", "2 hours") 
             )
customerDF = customerDF.withColumn("@timestamp", col("processingTime"))

# Read data from 'ecommerce_products' topic
productSchema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier", StringType(), True),
    StructField("rating", DoubleType(), True)
])
productDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "product_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", productSchema).alias("data")) \
    .select("data.*") \
    .withColumn("processingTime", current_timestamp())  # Add processing timestamp

productDF = productDF.withColumn("@timestamp", col("processingTime"))
productDF = productDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_transactions' topic
transactionSchema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),  
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True)
])
transactionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "transaction_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", transactionSchema).alias("data")) \
    .select("data.*")

transactionDF = transactionDF.withColumn("processingTime", current_timestamp())
transactionDF = transactionDF.withColumn("@timestamp", col("processingTime"))
transactionDF = transactionDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_product_views' topic
productViewSchema = StructType([
    StructField("view_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("view_duration", IntegerType(), True)
])
productViewDF = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                 .option("subscribe", "product_view_data")
                 .option("startingOffsets", "earliest")
                 .load()
                 .selectExpr("CAST(value AS STRING)")
                 .select(from_json("value", productViewSchema).alias("data"))
                 .select("data.*")
                 .withColumn("timestamp", col("timestamp").cast("timestamp"))
                 .withWatermark("timestamp", "1 hour")
                 )
productViewDF = productViewDF.withColumn("processingTime", current_timestamp())
productViewDF = productViewDF.withColumn("@timestamp", col("processingTime"))

# Read data from 'ecommerce_system_logs' topic
systemLogSchema = StructType([
    StructField("log_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("level", StringType(), True),
    StructField("message", StringType(), True)
])

systemLogDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "system_log_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", systemLogSchema).alias("data")) \
    .select("data.*")

systemLogDF = systemLogDF.withColumn("processingTime", current_timestamp())
systemLogDF = systemLogDF.withColumn("@timestamp", col("processingTime"))
systemLogDF = systemLogDF.withWatermark("processingTime", "2 hours")

# Read data from 'ecommerce_user_interactions' topic
userInteractionSchema = StructType([
    StructField("interaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("interaction_type", StringType(), True),
    StructField("details", StringType(), True)
])

userInteractionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "user_interaction_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", userInteractionSchema).alias("data")) \
    .select("data.*")

userInteractionDF = userInteractionDF.withColumn("processingTime", current_timestamp())
userInteractionDF = userInteractionDF.withColumn("@timestamp", date_format(col("processingTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
userInteractionDF = userInteractionDF.withWatermark("processingTime", "2 hours")

reviewsDF = userInteractionDF.filter(userInteractionDF["interaction_type"] == "review")
reviewsDF = reviewsDF.withColumn("sentiment", sentiment_udf(reviewsDF["details"]))
reviewsDF = reviewsDF[['product_id', 'sentiment', '@timestamp']]

#This analysis  focus on demographics and account activity.
customerAnalysisDF = (customerDF
                      .groupBy(
                          window(col("last_login"), "5 minutes"),  # Windowing based on last_login
                          "gender"
                      )
                      .agg(
                          count("customer_id").alias("total_customers"),
                          max("last_login").alias("last_activity")
                      )
                     )

customerAnalysisDF = customerAnalysisDF.withColumn(
    "unique_id",
    concat_ws("_", col("gender"), col("window.start").cast("string"))  # Access window.start explicitly
)
customerAnalysisDF = customerAnalysisDF.withColumn("processingTime", current_timestamp())
customerAnalysisDF = customerAnalysisDF.withColumn("@timestamp", date_format(col("processingTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

# Analyzing product popularity and stock status with windowing
productAnalysisDF = productDF \
    .groupBy(
        window(col("processingTime"), "5 minutes"),  # Window based on processingTime
        "category"
    ) \
    .agg(
        avg("price").alias("average_price"),
        sum("stock_quantity").alias("total_stock")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("average_price"),
        col("total_stock")
    )
productAnalysisDF = productAnalysisDF.withColumn(
    "unique_id",
    concat_ws("_", col("category"), col("window_start").cast("string"))
)

productAnalysisDF = productAnalysisDF.withColumn("processingTime", current_timestamp())
productAnalysisDF = productAnalysisDF.withColumn("@timestamp", date_format(col("processingTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))



# ordered customer activity in terms of the number of interactions they have with products, such as wishlist additions, reviews, or ratings.
customerActivityDF = (userInteractionDF
                      .groupBy("customer_id")
                      .agg(
                          count("interaction_id").alias("total_interactions"),
                          count(when(col("interaction_type") == "wishlist_addition", 1)).alias("wishlist_additions"),
                          count(when(col("interaction_type") == "review", 1)).alias("reviews"),
                          count(when(col("interaction_type") == "rating", 1)).alias("ratings")
                      ).orderBy(col("total_interactions").desc()) 
                     )
customerActivityDF = customerActivityDF.withColumn("@timestamp", current_timestamp())
top20CustomersDF = customerActivityDF.limit(20)


# low stock level alert
salesVelocityDF = transactionDF.groupBy(
    col("product_id")
).agg(
    avg("quantity").alias("average_daily_sales")
)

salesVelocityDF.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("sales_velocity") \
    .option("checkpointLocation", "/tmp/sales_velocity_checkpoint") \
    .start()

spark.sql("SELECT * FROM sales_velocity").show()

# Join with Product Data to include category
productWithThresholdDF = productDF.join(
    spark.sql("SELECT * FROM sales_velocity"),
    "product_id",
    "left_outer"
).withColumn(
    "threshold", col("average_daily_sales") * 2
).fillna({"threshold": 10})  # Default threshold

# Filter for Low Stock and Include Category
lowStockDF = productWithThresholdDF.filter(
    col("stock_quantity") < col("threshold")
).select(
    "product_id", "category", "name", "stock_quantity", "threshold", "processingTime"
).withColumn(
    "alert", lit("Low stock level detected")
)
lowStockDF = lowStockDF.withColumn(
    "unique_id",
    concat_ws("_", col("product_id"), col("processingTime").cast("string"))
)

lowStockDF = lowStockDF.withColumn("processingTime", current_timestamp())
lowStockDF = lowStockDF.withColumn("@timestamp", date_format(col("processingTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))



#number of customers retained - retention analysis
retentionDF = transactionDF.groupBy(
    col("customer_id")
).agg(
    count("transaction_id").alias("purchase_count")
).filter(
    col("purchase_count") > 1
).withColumn(
    "shopper_type",
    when((col("purchase_count") >= 2) & (col("purchase_count") <= 3), "Occasional Shoppers")
    .when((col("purchase_count") >= 4) & (col("purchase_count") <= 10), "Frequent Buyers")
    .when(col("purchase_count") > 10, "Loyal Customers")
    .otherwise("Unknown")
)
retentionDF = retentionDF.withColumn("processingTime", current_timestamp())
retentionDF = retentionDF.withColumn(
    "unique_id",
    concat_ws("_", col("customer_id"), col("processingTime").cast("string"))
)

retentionDF = retentionDF.withColumn("processingTime", current_timestamp())
retentionDF = retentionDF.withColumn("@timestamp", date_format(col("processingTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))


# Wait for any of the streams to finish
customerAnalysisDF.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/customeranalysis_analysis") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "customeranalysis_index") \
    .option("es.mapping.id", "unique_id") \
    .start() 
productAnalysisDF.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/productanalysis_analysis") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "productanalysis_index") \
    .option("es.mapping.id", "unique_id") \
    .start()
reviewsDF.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/review_analysis") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "review_index") \
    .start()
retentionDF.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/retention_analysis") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "retention_index") \
    .option("es.mapping.id", "unique_id") \
    .start()
lowStockDF.writeStream \
    .outputMode("update") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/lowstock_analysis") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "lowstock_index") \
    .option("es.mapping.id", "unique_id") \
    .start() 

top20CustomersDF.writeStream.format("console").outputMode("complete").start().awaitTermination()

spark.streams.awaitAnyTermination()
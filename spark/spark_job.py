import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *

# Load Kafka connection and topic details from environment variables
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_incidents")

# Define schema for incoming JSON payloads from Kafka
# This matches the structure defined in the producer
schema = StructType([
    StructField("trafficModelId", StringType()),
    StructField("id", StringType()),
    StructField("typeCode", StringType()),
    StructField("iconCode", StringType()),
    StructField("description", StringType()),
    StructField("comment", StringType()),
    StructField("from", StringType()),
    StructField("to", StringType()),
    StructField("road", StringType()),
    StructField("delaySeconds", IntegerType()),
    StructField("position", MapType(StringType(), StringType())),
    StructField("area_hint", StringType()),
    StructField("_raw", MapType(StringType(), StringType())),
])

# Initialize SparkSession for structured streaming
spark = (
    SparkSession.builder
    .appName("TrafficIncidentsConsole")  # Job name shown in Spark UI
    .getOrCreate()
)

# Read Kafka topic as a streaming source
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")   # Only consume new messages
    .option("failOnDataLoss", "false")     # Handle any Kafka partition loss gracefully
    .load()
)

# Parse and flatten Kafka JSON message payloads
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")           # Kafka value is a byte string
       .select(from_json(col("json"), schema).alias("e"))     # Parse JSON into structured format
       .select("e.*")                                          # Flatten out nested struct
       .withColumn("event_ts", to_timestamp(col("_raw")["st"], "yyyy-MM-dd'T'HH:mm:ssX"))  
       # Extract and convert original timestamp if available
)

# Write parsed records to the console sink for real-time visibility
query = (
    parsed.writeStream
    .format("console")
    .option("truncate", "false")  # Show full content of each row
    .outputMode("append")         # Print new rows as they arrive
    .start()
)

# Keep the stream running until manually stopped
query.awaitTermination()

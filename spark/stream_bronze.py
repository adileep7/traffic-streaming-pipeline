import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

# Load environment variables (e.g., Kafka and Delta paths)
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_incidents")
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
CHECKPOINT = os.getenv("BRONZE_CHECKPOINT", "./checkpoints/bronze_incidents")

# Define schema for the incoming JSON messages from Kafka
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

# Configure Spark session with Delta Lake support
builder = (
    SparkSession.builder
      .appName("TrafficIncidentsBronze")  # Spark UI / job label
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.streaming.schemaInference", "true")  # Let Spark infer schema on first batch
      .config("spark.sql.shuffle.partitions", "1")            # Useful for small/local workloads
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Subscribe to Kafka topic as streaming input
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")     # Only process new data
    .option("failOnDataLoss", "false")       # Ignore lost offsets on restart
    .load()
)

# Deserialize Kafka JSON payload and extract structured fields
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")               # Kafka value is a byte stream
       .select(from_json(col("json"), schema).alias("e"))         # Parse into structured format
       .select("e.*")                                              # Flatten out the struct
       .withColumn("event_ts", to_timestamp(col("_raw")["st"], "yyyy-MM-dd'T'HH:mm:ssX"))
       # Derive event timestamp from raw incident data
)

# Write parsed data to the Bronze Delta Lake table
(output := parsed.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT)             # Enables fault tolerance
    .outputMode("append")                                 # Append-only write for Bronze
    .trigger(processingTime="10 seconds")                 # Force small, frequent micro-batches
    .start(os.path.join(BRONZE_PATH, "incidents"))        # Delta table path
).awaitTermination()

import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date,
    coalesce, current_timestamp
)
from delta import configure_spark_with_delta_pip

# Load environment variables from .env file (paths, configs)
load_dotenv()

# Define source, sink, and checkpoint locations
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver")
CHECKPOINT  = os.getenv("SILVER_CHECKPOINT", "./checkpoints/silver_incidents")

# Set up SparkSession with Delta Lake support
builder = (
    SparkSession.builder
      .appName("TrafficIncidentsSilver")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "1")  # Tuning for small-scale processing
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read streaming data from the Bronze Delta table
bronze = spark.readStream.format("delta").load(f"{BRONZE_PATH}/incidents")

# Perform basic transformation and normalization
silver = (
    bronze
    .select(
        col("id").alias("incident_id"),
        col("trafficModelId").alias("model_id"),
        "typeCode", "iconCode", "description", "comment",
        "from", "to", "road",
        col("delaySeconds").cast("int").alias("delay_seconds"),
        "position",
        "area_hint",
        "event_ts"
    )
    # Ensure all records have a valid timestamp (fallback to current time)
    .withColumn("event_ts", coalesce(col("event_ts"), to_timestamp(current_timestamp())))
    # Add derived date column for partitioning
    .withColumn("event_date", to_date(col("event_ts")))
)

# Write to Silver Delta table in append mode, partitioned by date
(
    silver.writeStream
      .format("delta")
      .option("checkpointLocation", CHECKPOINT)  # Required for fault-tolerant writes
      .partitionBy("event_date")                 # Optimize downstream querying
      .outputMode("append")
      .trigger(processingTime="10 seconds")      # Trigger frequency
      .start(f"{SILVER_PATH}/incidents")
      .awaitTermination()
)

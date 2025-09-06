import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, coalesce, current_timestamp
from delta import configure_spark_with_delta_pip

load_dotenv()

BRONZE_PATH = os.getenv("BRONZE_PATH","./data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH","./data/silver")
CHECKPOINT  = os.getenv("SILVER_CHECKPOINT","./checkpoints/silver_incidents")

builder = (
    SparkSession.builder
      .appName("TrafficIncidentsSilver")
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions","1")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronze = spark.readStream.format("delta").load(f"{BRONZE_PATH}/incidents")

silver = (bronze
    .select(
        col("id").alias("incident_id"),
        col("trafficModelId").alias("model_id"),
        "typeCode","iconCode","description","comment","from","to","road",
        col("delaySeconds").cast("int").alias("delay_seconds"),
        col("position"),
        col("area_hint"),
        col("event_ts")
    )
    .withColumn("event_ts", coalesce(col("event_ts"), to_timestamp(current_timestamp())))
    .withColumn("event_date", to_date(col("event_ts")))
)

(silver.writeStream
  .format("delta")
  .option("checkpointLocation", CHECKPOINT)
  .partitionBy("event_date")
  .outputMode("append")
  .trigger(processingTime="10 seconds")
  .start(f"{SILVER_PATH}/incidents")
  .awaitTermination())

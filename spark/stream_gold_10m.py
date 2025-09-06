import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, avg
from delta import configure_spark_with_delta_pip

load_dotenv()

SILVER_PATH = os.getenv("SILVER_PATH","./data/silver")
GOLD_PATH   = os.getenv("GOLD_PATH","./data/gold")
CHECKPOINT  = os.getenv("GOLD_CHECKPOINT","./checkpoints/gold_incidents_10m")

builder = (
    SparkSession.builder
      .appName("TrafficIncidentsGold10m")
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions","1")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver = spark.readStream.format("delta").load(f"{SILVER_PATH}/incidents")

agg = (
    silver
      .where(col("event_ts").isNotNull())
      .groupBy(
          window(col("event_ts"), "10 minutes").alias("w"),
          col("typeCode").alias("type_code"),
          col("road")
      )
      .agg(
          count("*").alias("event_count"),
          avg("delay_seconds").alias("avg_delay_seconds")
      )
      .select(
          col("w.start").alias("window_start"),
          col("w.end").alias("window_end"),
          "type_code",
          "road",
          "event_count",
          "avg_delay_seconds"
      )
)

(agg.writeStream
   .format("delta")
   .option("checkpointLocation", CHECKPOINT)
   .outputMode("append")
   .trigger(processingTime="10 seconds")
   .start(f"{GOLD_PATH}/agg_incidents_10m")
   .awaitTermination())

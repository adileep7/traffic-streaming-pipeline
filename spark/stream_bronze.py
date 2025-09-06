import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

load_dotenv()  # <-- load .env so KAFKA_BOOTSTRAP/paths apply

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_incidents")
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
CHECKPOINT = os.getenv("BRONZE_CHECKPOINT", "./checkpoints/bronze_incidents")

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

builder = (
    SparkSession.builder
      .appName("TrafficIncidentsBronze")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.shuffle.partitions", "1")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("e"))
       .select("e.*")
       .withColumn("event_ts", to_timestamp(col("_raw")["st"], "yyyy-MM-dd'T'HH:mm:ssX"))
)

(output := parsed.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(processingTime="10 seconds")  # force frequent commits
    .start(os.path.join(BRONZE_PATH, "incidents"))
).awaitTermination()

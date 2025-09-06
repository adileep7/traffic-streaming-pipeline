import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_incidents")

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

spark = (
    SparkSession.builder
    .appName("TrafficIncidentsConsole")
    .getOrCreate()
)

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

query = (
    parsed.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start()
)

query.awaitTermination()

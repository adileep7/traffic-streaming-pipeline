from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg

SILVER_PATH = "./data/silver/incidents"
GOLD_PATH = "./data/gold"
CHECKPOINT = "./checkpoints/gold_incidents_10m"

def main():
    spark = (
        SparkSession.builder
        .appName("TrafficIncidentsGold10m")
        # Delta configs (safe to include even if you also set them via spark-submit)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Optional: speedups for streaming state ops
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    # Read the silver Delta table as a stream and add a watermark so we can use append mode
    silver = (
        spark.readStream
             .format("delta")
             .load(SILVER_PATH)
             .withWatermark("event_ts", "30 minutes")   # ← key fix
    )

    # 10-minute tumbling window aggregates
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

    # Write the gold Delta table as a stream in append mode
    query = (
        agg.writeStream
           .format("delta")
           .outputMode("append")                         # works because we added a watermark
           .option("checkpointLocation", CHECKPOINT)
           .trigger(processingTime="10 seconds")         # adjust as you like
           .start(f"{GOLD_PATH}/agg_incidents_10m")
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()

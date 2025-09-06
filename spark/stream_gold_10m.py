from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg

# File system locations for input, output, and streaming checkpointing
SILVER_PATH = "./data/silver/incidents"
GOLD_PATH = "./data/gold"
CHECKPOINT = "./checkpoints/gold_incidents_10m"

def main():
    # Initialize SparkSession with Delta Lake support and tuning options
    spark = (
        SparkSession.builder
        .appName("TrafficIncidentsGold10m")  # Job name in Spark UI
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")  # Optimize shuffles for grouping/window ops
        .getOrCreate()
    )

    # Read the Silver-level Delta table as a streaming source
    silver = (
        spark.readStream
             .format("delta")
             .load(SILVER_PATH)
             .withWatermark("event_ts", "30 minutes")  # Required for windowed aggregation in append mode
    )

    # Perform 10-minute tumbling window aggregations by road and incident type
    agg = (
        silver
        .where(col("event_ts").isNotNull())  # Filter out records missing timestamps
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

    # Write aggregated results to a Delta Gold table as a streaming sink
    query = (
        agg.writeStream
           .format("delta")
           .outputMode("append")  # Works with watermark + tumbling window
           .option("checkpointLocation", CHECKPOINT)  # Ensures exactly-once fault-tolerant writes
           .trigger(processingTime="10 seconds")      # Controls how often output is written
           .start(f"{GOLD_PATH}/agg_incidents_10m")
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()

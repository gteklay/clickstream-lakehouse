# ~/clickstream-lakehouse/spark-jobs/spark_lakehouse_ingest.py
import sys
import time
from pyspark.sql import SparkSession  # type: ignore[reportMissingImports]
from pyspark.sql.functions import col, from_json, expr, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

print("⚡ PySpark Distributed Streaming Engine initializing connection arrays...")

# ==============================================================================
# 易 DISTRIBUTED CLUSTER INITIALIZATION & ICEBERG CATALOG ARCHITECTURE
# ==============================================================================
# We natively initialize Spark and configure the explicit Apache Iceberg 
# catalog pointer tags to mount our local MinIO S3 storage buckets securely.
# ~/clickstream-lakehouse/spark-jobs/ingest.py
from pyspark.sql import SparkSession  # type: ignore[reportMissingImports]

print("⚡ PySpark Engine: FORCING 100% PRODUCTION POSTGRESQL NETWORK CATALOG...")

spark = SparkSession.builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Confirming Spark configuration...")
print(spark.conf.get("spark.sql.catalog.local_lakehouse"))
# Force namespace initialization inside PostgreSQL ledger database on boot
spark.sql("CREATE DATABASE IF NOT EXISTS local_lakehouse.click_db;")
print(" Spark Session successfully compiled! Postgres network connection locked.")


# ==============================================================================
# ️ AUTOMATED TABLE DDL SCHEMA INITIALIZATION 
# ==============================================================================
# This creates your open table tracking definitions directly on your MinIO storage 
# disk space if they don't exist, preventing streaming initialization dropouts!
print("隣 Verifying database table structures inside local object storage...")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local_lakehouse.click_db.bronze_clickstream (
        event_id STRING,
        event_timestamp TIMESTAMP,
        user_id STRING,
        session_id STRING,
        ip_address STRING,
        device_type STRING,
        location_country STRING,
        page_url STRING,
        action_type STRING,
        item_id STRING,
        item_price DOUBLE,
        billing_country_match BOOLEAN
    ) 
    USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local_lakehouse.click_db.gold_fraud_alerts (
        event_window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,
        ip_address STRING,
        location_country STRING,
        device_type STRING,
        event_count LONG,
        is_bot_attack BOOLEAN
    ) 
    USING iceberg
""")

print("✅ Iceberg Table structures confirmed healthy!")

# ==============================================================================
#  CONNECTING TO THE REAL-TIME KAFKA 4.x COMPACT BROKER
# ==============================================================================
# ... rest of your kafka_raw_stream and processing code blocks continue here ...


# ==============================================================================
# ️ STREAM BOUNDARY SCHEMA GOVERNANCE
# ==============================================================================
# Define a rigid typing struct mapping to safely parse incoming Kafka event fields
clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location_country", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("item_price", DoubleType(), True),
    StructField("billing_country_match", BooleanType(), True)
])

# ==============================================================================
#  CONNECTING TO THE KAFKA 4.x REAL-TIME BUFFER
# ==============================================================================
# Connect straight to the internal Docker network KRaft cluster topic channel
kafka_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web.clickstream") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5000) \
    .option("failOnDataLoss", "false") \
    .load()

# Convert binary raw Kafka payloads cleanly into structured, typed string maps
parsed_stream = kafka_raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), clickstream_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

# ==============================================================================
# 雷 BRONZE TIER: PERSIST RAW EVENT FILES TO ICEBERG
# ==============================================================================
# Writes the append-only raw stream records straight to your MinIO storage tables
bronze_query = (parsed_stream.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("failOnDataLoss", "false")
    # 2. Inject your tracking checkpoints NEXT
    .option("checkpointLocation", "file:///opt/spark/warehouse/checkpoints/bronze")
    # 3. Call toTable() as the ABSOLUTE FINAL execution method! (Omit .start())
    .toTable("local_lakehouse.click_db.bronze_clickstream"))

# ==============================================================================
# 賂 SILVER TIER: STATEFUL WINDOW AGGREGATION & BOT ISOLATION
# ==============================================================================
# Enforce a strict 10-minute Watermark guardrail to handle late network packages.
# Evaluates moving 1-minute sliding windows to count user click velocities.
bot_isolation_stream = (parsed_stream
    .withWatermark("event_timestamp", "1 minutes")
    .groupBy(
        window(col("event_timestamp"), "1 minute", "30 seconds"),
        col("ip_address"),
        col("location_country"),
        col("device_type")
    )
    .count()
    .withColumn("is_bot_attack", expr("count > 20")) # Flag IP if clicks spike past 20 inside a window
    .withColumnRenamed("window", "event_window")  # Rename for sql keyword conflict avoidance
    .withColumnRenamed("count", "event_count"))  # Rename for sql keyword conflict avoidance

# ==============================================================================
# 磊 GOLD TIER: STREAM ANOMALY ALERTS TO ICEBERG FOR BI DASHBOARDS
# ==============================================================================
# Filters for true bot attack alerts and streams them to high-value analytical zones
gold_query = (bot_isolation_stream
    .filter(col("is_bot_attack") == True)
    .writeStream
    .format("iceberg")
    .outputMode("complete")
    .trigger(processingTime="15 seconds")
    #.option("failOnDataLoss", "false")
    # 2. Inject your tracking checkpoints NEXT
    .option("checkpointLocation", "file:///opt/spark/warehouse/checkpoints//gold")
    
    # 3. Call toTable() as the ABSOLUTE FINAL execution method! (Omit .start())
    .toTable("local_lakehouse.click_db.gold_fraud_alerts"))

# Keep streaming operations running continuously
global_running_total_rows = 0
ROW_LIMIT_CEILING = 4000000

try:
    while bronze_query.isActive and gold_query.isActive:
        time.sleep(10)
        if bronze_query.recentProgress:
            latest_batch = bronze_query.recentProgress[-1]["numInputRows"]
            if latest_batch > 0:
                global_running_total_rows += latest_batch
                print(f" Bronze +{latest_batch} rows | Total: {global_running_total_rows:,}")
        
        if global_running_total_rows >= ROW_LIMIT_CEILING:
            print(" Storage guardrail reached. Stopping streams...")
            bronze_query.stop()
            gold_query.stop()
            break

except KeyboardInterrupt:
    print("\n Manual interrupt detected. Shutting down streams cleanly...")
    bronze_query.stop()
    gold_query.stop()
except Exception as e:
    print(f"⚠️ Streaming error encountered: {e}")
    bronze_query.stop()
    gold_query.stop()
#spark.streams.awaitAnyTermination()
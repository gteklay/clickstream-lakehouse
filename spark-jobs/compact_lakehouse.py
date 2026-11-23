from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IcebergLakehouseMaintenance").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

tables = [
    "local_lakehouse.click_db.bronze_clickstream",
    "local_lakehouse.click_db.gold_fraud_alerts"
]

# Retain snapshots from the last 1 hour for time-travel; expire older ones
cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

for table in tables:
    print(f"\n粒 Compacting small Parquet files for {table}...")
    spark.sql(f"CALL local_lakehouse.system.rewrite_data_files(table => '{table}')").show()
    
    print(f"️ Expiring snapshots older than {cutoff_time} for {table}...")
    spark.sql(f"CALL local_lakehouse.system.expire_snapshots(table => '{table}', older_than => TIMESTAMP '{cutoff_time}')").show()
    
    print(f"識 Removing orphaned data files for {table}...")
    spark.sql(f"CALL local_lakehouse.system.remove_orphan_files(table => '{table}')").show()

print("\n Iceberg lakehouse maintenance completed successfully!")
spark.stop()

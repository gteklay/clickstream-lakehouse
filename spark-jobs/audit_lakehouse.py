import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BRONZE_TABLE = "local_lakehouse.click_db.bronze_clickstream"
GOLD_TABLE = "local_lakehouse.click_db.gold_fraud_alerts"


def audit_data_quality(spark: SparkSession, bronze_table: str, gold_table: str) -> int:
    """Validates table completeness (non-zero row counts) and null key integrity."""
    bronze_df = spark.table(bronze_table)
    gold_df = spark.table(gold_table)

    bronze_count = bronze_df.count()
    gold_count = gold_df.count()

    assert bronze_count > 0, f"❌ AUDIT FAILED: {bronze_table} is empty!"
    assert gold_count > 0, f"❌ AUDIT FAILED: {gold_table} is empty!"

    null_keys = bronze_df.filter(
        col("event_id").isNull() | col("ip_address").isNull()
    ).count()
    assert null_keys == 0, f"❌ AUDIT FAILED: Found {null_keys} rows with null event_id or ip_address!"

    print(
        f"✅ [1/3] Data Quality Passed | Bronze Rows: {bronze_count:,} | "
        f"Gold Rows: {gold_count:,} | Null Keys: 0\n"
    )
    return bronze_count


def audit_physical_files(spark: SparkSession, tables: list[str]) -> None:
    """Inspects Iceberg .files metadata to track Parquet file counts and storage size."""
    print(" [2/3] Iceberg Physical File Footprint:")
    for table in tables:
        stats = spark.sql(f"""
            SELECT 
                COUNT(*) AS file_count, 
                COALESCE(ROUND(SUM(file_size_in_bytes) / 1024 / 1024, 2), 0) AS total_mb 
            FROM {table}.files
        """).collect()[0]
        print(f"   • {table}: {stats['file_count']} Parquet files ({stats['total_mb']} MB)")
    print()


def audit_snapshots_and_time_travel(spark: SparkSession, table: str, current_count: int) -> None:
    """Audits Iceberg .snapshots history and verifies time-travel queries against the earliest snapshot."""
    print(f"️ [3/3] Iceberg Snapshot & Time-Travel Audit ({table}):")
    snapshots = spark.sql(f"""
        SELECT committed_at, snapshot_id, operation 
        FROM {table}.snapshots 
        ORDER BY committed_at ASC
    """).collect()

    print(f"   • Total Active Snapshots: {len(snapshots)}")
    if len(snapshots) >= 2:
        first_snapshot_id = snapshots[0]["snapshot_id"]
        historical_count = (
            spark.read.option("snapshot-id", first_snapshot_id)
            .table(table)
            .count()
        )
        print(
            f"   • Time-Travel to First Snapshot ({first_snapshot_id}): "
            f"{historical_count:,} rows vs. Current: {current_count:,} rows"
        )
    else:
        print("   • Only 1 snapshot present; time-travel comparison requires at least 2 commits.")
    print()


def main() -> None:
    spark = SparkSession.builder.appName("IcebergLakehouseAudit").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(" Starting Clickstream Lakehouse Audit...\n")
    try:
        current_bronze_count = audit_data_quality(spark, BRONZE_TABLE, GOLD_TABLE)
        audit_physical_files(spark, [BRONZE_TABLE, GOLD_TABLE])
        audit_snapshots_and_time_travel(spark, BRONZE_TABLE, current_bronze_count)
        print(" ALL DATA QUALITY & ICEBERG METADATA AUDITS PASSED!")

    except AssertionError as ae:
        print(f"\n{ae}")
        sys.exit(1)
    except Exception as e:
        print(f"\n⚠️ Unexpected audit error: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

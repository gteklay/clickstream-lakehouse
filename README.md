# Real-Time E-Commerce Clickstream & Fraud Detection Lakehouse

An end-to-end, real-time data engineering pipeline built with **Apache Kafka (KRaft)**, **Apache Spark Structured Streaming**, **Apache Iceberg**, **PostgreSQL**, **Trino**, and **Apache Superset**.

This project ingests live e-commerce clickstream events, detects suspicious bot checkout bursts in real time using a **Medallion Architecture (Bronze and Gold)**, and displays live security metrics on an interactive dashboard.

---

## ️ Architecture Overview

```text
[Python Event Generator] 
        │ (JSON Clickstream + Bot Attack Bursts)
        ▼
[Apache Kafka (KRaft Mode)] 
        │ (Throttled Micro-Batches)
        ▼
[Apache Spark 4.0 Structured Streaming]
   ├──► Bronze Table: Raw clickstream events (Append-only)
   └──► Gold Table: 1-minute sliding window fraud alerts (Watermarked)
        │
        ├──► Storage: Apache Iceberg (Snappy Parquet files)
        └──► Catalog: PostgreSQL 16 (JDBC Metadata Ledger)
        │
        ▼
[Trino Query Engine] ◄──► [Apache Superset Dashboard]
```

### Tech Stack
* **Data Ingestion:** Python, Apache Kafka (KRaft mode — no Zookeeper)
* **Stream Processing:** Apache Spark 4.0 (PySpark Structured Streaming)
* **Lakehouse Storage & Catalog:** Apache Iceberg 1.12, PostgreSQL 16 (JDBC Catalog)
* **Query & Visualization:** Trino, Apache Superset
* **Infrastructure:** Docker, Docker Compose, Ubuntu Linux VM

---
##  Project Structure

```text
clickstream-lakehouse/
├── generator/
│   ├── Dockerfile                     # Builds the Python clickstream event producer container
│   ├── producer.py                    # Generates normal user traffic and 50-event bot bursts
│   └── requirements.txt               # Python dependencies for the Kafka producer
├── infra/
│   ├── docker-compose.yaml            # Runs all containers with healthchecks and shared volumes
│   ├── trino-catalog/
│   │   └── local_lakehouse.properties # Connects Trino to the PostgreSQL Iceberg catalog
│   ├── postgres-data/                 # Local PostgreSQL metadata files (ignored by Git)
│   └── kafka-data/                    # Local Kafka topic logs (ignored by Git)
├── spark-jobs/
│   ├── ingest.py                      # Main PySpark streaming job (Bronze and Gold tables)
│   ├── compact_lakehouse.py           # Merges small Parquet files and removes old snapshots
│   ├── audit_lakehouse.py             # Checks row counts, null values, file sizes, and time travel
│   └── spark-defaults.conf            # Spark and Iceberg catalog settings
├── warehouse/                         # Shared Iceberg Parquet files and checkpoints (ignored by Git)
├── .gitignore                         # Keeps large data folders and logs out of Git
└── README.md                          # Project documentation and setup guide
```

---

##  How the Data Pipeline Works

1. **Data Generator (`producer.py`):**
   * Creates realistic user browsing and checkout events in JSON format.
   * Periodically injects 50 rapid checkout attempts from the same IP address (`billing_country_match = FALSE`) to simulate a bot attack.
2. **Bronze Layer (`local_lakehouse.click_db.bronze_clickstream`):**
   * Reads raw events from Kafka in throttled batches (`maxOffsetsPerTrigger`) and saves them as compressed Parquet files in Apache Iceberg.
3. **Gold Layer (`local_lakehouse.click_db.gold_fraud_alerts`):**
   * Uses event-time watermarking and 1-minute sliding windows (sliding every 30 seconds) grouped by `ip_address`, `location_country`, and `device_type`.
   * Flags an IP address as a bot attack (`is_bot_attack = true`) when failed attempts exceed 20 events inside a window.
4. **Maintenance & Quality Checks (`compact_lakehouse.py` & `audit_lakehouse.py`):**
   * **Compaction:** Combines many small streaming Parquet files into larger files (`rewrite_data_files`) and deletes expired snapshots (`expire_snapshots`) to keep queries fast.
   * **Data Audit:** Verifies non-zero row counts, checks for null keys, inspects physical file footprints, and tests Iceberg time-travel queries across historical snapshots.

---

## ️ Key Problems Solved

* **Spark and Trino File Path Mismatch (`file:/` vs. `file:///`):**
  * *Problem:* Spark writes local file paths with one slash (`file:/opt/...`), while Trino expects three slashes (`file:///opt/...`), causing `Metadata not found` errors.
  * *Solution:* Enabled `fs.hadoop.enabled=true` in Trino's Iceberg catalog properties and aligned shared volume mounts across Spark and Trino containers.
* **Disk Space Protection & Backpressure:**
  * *Problem:* Reading large Kafka backlogs all at once risked filling up the VM disk.
  * *Solution:* Added `maxOffsetsPerTrigger` micro-batch limits, Kafka log retention caps, and a 4,000,000-row stop guardrail in `ingest.py`.
* **Container Startup Order:**
  * *Problem:* Spark and Trino failed if they booted before PostgreSQL and Kafka were ready.
  * *Solution:* Added Docker Compose `healthcheck` probes (`pg_isready`, `nc -z`) with `condition: service_healthy` rules.

---

##  How to Run

### 1. Start the Containers
```bash
cd infra
docker compose up -d
```

### 2. Start Real-Time Ingestion
```bash
docker exec -it lakehouse_spark_master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,org.postgresql:postgresql:42.7.2 \
  /opt/spark-apps/ingest.py
```

### 3. Run Data Audit & Compaction (After Ingestion)
```bash
# Check data quality, file counts, and Iceberg time travel
docker exec -it lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master "local" \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,org.postgresql:postgresql:42.7.2 \
  /opt/spark-apps/audit_lakehouse.py

# Compact small Parquet files and clean up old snapshots
docker exec -it lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master "local" \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,org.postgresql:postgresql:42.7.2 \
  /opt/spark-apps/compact_lakehouse.py
```

### 4. View the Dashboard in Apache Superset
1. Open `http://<YOUR_VM_IP>:8088` in your browser (default login: `admin` / `admin`).
2. Connect to Trino using the SQLAlchemy URI:
   ```text
   trino://admin@trino:8080/local_lakehouse/click_db
   ```
3. Open **SQL Lab** or your **Clickstream Security Center** dashboard to explore blocked bot bursts by country, device type, and attacker IP.

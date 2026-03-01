from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# ============================================================
# 1. Partitioned Sales Table (Demonstrates Level 2 Nodes)
# ============================================================
spark.sql("DROP TABLE IF EXISTS default.sales_partitioned")

spark.sql(
    """
CREATE TABLE default.sales_partitioned (
    sale_id INT,
    category STRING,
    amount DOUBLE,
    sale_date DATE
) 
USING iceberg
PARTITIONED BY (category, days(sale_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read'
)
"""
)

# Insert data into multiple partitions (Category A and B)
spark.sql(
    """
INSERT INTO default.sales_partitioned VALUES 
(1, 'Electronics', 500.0, CAST('2026-02-20' AS DATE)),
(2, 'Electronics', 150.0, CAST('2026-02-20' AS DATE)),
(3, 'Furniture', 1200.0, CAST('2026-02-21' AS DATE))
"""
)

# Add more data to trigger a second manifest
spark.sql(
    """
INSERT INTO default.sales_partitioned VALUES 
(4, 'Electronics', 200.0, CAST('2026-02-21' AS DATE))
"""
)

# Perform an UPDATE to create Equality Deletes (RED nodes in your graph)
spark.sql("UPDATE default.sales_partitioned SET amount = 550.0 WHERE sale_id = 1")


# ============================================================
# 2. High-Churn Logs Table (Demonstrates Snapshot History)
# ============================================================
spark.sql("DROP TABLE IF EXISTS default.system_logs")

spark.sql(
    """
CREATE TABLE default.system_logs (
    log_id BIGINT,
    level STRING,
    message STRING,
    ts TIMESTAMP
) 
USING iceberg
TBLPROPERTIES ('format-version' = '2')
"""
)

# Rapid fire inserts to create multiple snapshots quickly
for i in range(3):
    spark.sql(
        f"INSERT INTO default.system_logs VALUES ({i}, 'INFO', 'Heartbeat {i}', current_timestamp())"
    )
    time.sleep(1)  # Ensure unique timestamps for your arrow filtering tests

print("Test Data Ready!")
print("Try viewing: 'default.sales_partitioned' or 'default.system_logs'")

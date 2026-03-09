from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

spark.sql("DROP TABLE IF EXISTS default.logging")
spark.sql("""
    CREATE TABLE default.logging (
        event_id    INT,
        event_name  STRING,
        event_ts    TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (hour(event_ts))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read'
    )
""")
print("✅ Created table `default.logging`, partitioned by hour(event_ts)\n")

# ─────────────────────────────────────────────
# 2. Insert data for Hour 10 (10:00–10:59)
# ─────────────────────────────────────────────
hour10_data = [
    (1, "login", "2025-06-15 10:05:00"),
    (2, "click", "2025-06-15 10:20:00"),
    (3, "purchase", "2025-06-15 10:45:00"),
]
df_hour10 = spark.createDataFrame(
    hour10_data, ["event_id", "event_name", "event_ts_str"]
)
df_hour10 = df_hour10.withColumn("event_ts", F.to_timestamp("event_ts_str")).drop(
    "event_ts_str"
)
df_hour10.writeTo("default.logging").overwritePartitions()
print("✅ Inserted 3 rows into hour 10 partition")
spark.table("default.logging").show(1000, False)

# ─────────────────────────────────────────────
# 2.5 Schema evolution: add a column, rename one
# ─────────────────────────────────────────────
print("\n📋 Schema before evolution:")
spark.table("default.logging").printSchema()

spark.sql("ALTER TABLE default.logging ADD COLUMNS (event_source STRING)")
print("✅ Added column: event_source")

spark.sql("ALTER TABLE default.logging RENAME COLUMN event_name TO event_type")
print("✅ Renamed column: event_name → event_type")

print("\n📋 Schema after evolution:")
spark.table("default.logging").printSchema()
spark.table("default.logging").show(1000, False)

# ─────────────────────────────────────────────
# 3. Insert data for Hour 11 (11:00–11:59)
#    now using the new schema
# ─────────────────────────────────────────────
hour11_data = [
    (4, "logout", "2025-06-15 11:10:00", "mobile"),
    (5, "signup", "2025-06-15 11:30:00", "web"),
]
df_hour11 = spark.createDataFrame(
    hour11_data, ["event_id", "event_type", "event_ts_str", "event_source"]
)
df_hour11 = df_hour11.withColumn("event_ts", F.to_timestamp("event_ts_str")).drop(
    "event_ts_str"
)
df_hour11.writeTo("default.logging").append()
print("✅ Inserted 2 rows into hour 11 partition (with new columns)")
spark.table("default.logging").show(1000, False)

# ─────────────────────────────────────────────
# 4. Overwrite ONLY the hour-10 partition
#    with completely new replacement data
# ─────────────────────────────────────────────
hour10_replacement = [
    (100, "corrected_login", "2025-06-15 10:05:00", "api"),
    (101, "corrected_purchase", "2025-06-15 10:45:00", "api"),
]
df_hour10_new = spark.createDataFrame(
    hour10_replacement, ["event_id", "event_type", "event_ts_str", "event_source"]
)
df_hour10_new = df_hour10_new.withColumn(
    "event_ts", F.to_timestamp("event_ts_str")
).drop("event_ts_str")
df_hour10_new.writeTo("default.logging").overwritePartitions()
print("✅ Overwrote hour 10 partition with corrected data")
print("   (Hour 11 remains untouched)\n")

print("📋 Final table contents:")
spark.table("default.logging").show(1000, False)

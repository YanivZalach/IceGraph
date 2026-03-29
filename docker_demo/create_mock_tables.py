import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()

spark.sql("DROP TABLE IF EXISTS default.events")
spark.sql(
    """
    CREATE TABLE default.events (
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
"""
)
print("✅ Created table `default.events`, partitioned by hour(event_ts)\n")

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
df_hour10.writeTo("default.events").overwritePartitions()
print("✅ Inserted 3 rows into hour 10 partition")
spark.table("default.events").show(1000, False)

# ─────────────────────────────────────────────
# 2.5 Schema evolution: add a column, rename one
# ─────────────────────────────────────────────
print("\n📋 Schema before evolution:")
spark.table("default.events").printSchema()

spark.sql("ALTER TABLE default.events ADD COLUMNS (event_source STRING)")
print("✅ Added column: event_source")

spark.sql("ALTER TABLE default.events RENAME COLUMN event_name TO event_type")
print("✅ Renamed column: event_name → event_type")

print("\n📋 Schema after evolution:")
spark.table("default.events").printSchema()
spark.table("default.events").show(1000, False)

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
df_hour11.writeTo("default.events").append()
print("✅ Inserted 2 rows into hour 11 partition (with new columns)")
spark.table("default.events").show(1000, False)

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
df_hour10_new.writeTo("default.events").overwritePartitions()
print("✅ Overwrote hour 10 partition with corrected data")
print("   (Hour 11 remains untouched)\n")

print("📋 Final table contents:")
spark.table("default.events").show(1000, False)
# ─────────────────────────────────────────────
# 5. Create 2 branches from the current snapshot
# ─────────────────────────────────────────────
spark.sql("ALTER TABLE default.events CREATE BRANCH audit_branch")
spark.sql("ALTER TABLE default.events CREATE BRANCH audit_branch2")
print(
    "✅ Created branch 'audit_branch' and branch 'audit_branch2' from current snapshot"
)

# Write some data to main that won't appear on the branch
experimental_data = [
    (200, "experiment", "2025-06-15 12:00:00", "test"),
]
df_exp = spark.createDataFrame(
    experimental_data, ["event_id", "event_type", "event_ts_str", "event_source"]
)
df_exp = df_exp.withColumn("event_ts", F.to_timestamp("event_ts_str")).drop(
    "event_ts_str"
)
df_exp.writeTo("default.events").overwritePartitions()
print("✅ Overwrote hour 12 partition on main (after branching)")

# ─────────────────────────────────────────────
# 6. Write data to the branch
# ─────────────────────────────────────────────
branch_data = [
    (300, "audit_fix", "2025-06-15 13:15:00", "audit"),
    (301, "audit_review", "2025-06-15 13:45:00", "audit"),
]
df_branch = spark.createDataFrame(
    branch_data, ["event_id", "event_type", "event_ts_str", "event_source"]
)
df_branch = df_branch.withColumn("event_ts", F.to_timestamp("event_ts_str")).drop(
    "event_ts_str"
)
df_branch.writeTo("default.events.branch_audit_branch").overwritePartitions()
print("✅ Overwrote hour 13 partition on audit_branch")

# ─────────────────────────────────────────────
# 7. Compare main vs branch
# ─────────────────────────────────────────────
print("\n📋 Main branch:")
spark.table("default.events").show(1000, False)

print("📋 audit_branch:")
spark.read.option("branch", "audit_branch").table("default.events").show(1000, False)

# ─────────────────────────────────────────────
# 8. Create a second branch from current main
# ─────────────────────────────────────────────
spark.sql("ALTER TABLE default.events CREATE BRANCH feature_branch")
print("✅ Created branch 'feature_branch' from current main snapshot")

# Write data to feature_branch
feature_data = [
    (400, "feature_test", "2025-06-15 14:10:00", "staging"),
    (401, "feature_rollout", "2025-06-15 14:30:00", "staging"),
    (402, "feature_flag", "2025-06-15 14:55:00", "staging"),
]
df_feature = spark.createDataFrame(
    feature_data, ["event_id", "event_type", "event_ts_str", "event_source"]
)
df_feature = df_feature.withColumn("event_ts", F.to_timestamp("event_ts_str")).drop(
    "event_ts_str"
)
df_feature.writeTo("default.events.branch_feature_branch").overwritePartitions()
print("✅ Overwrote hour 14 partition on feature_branch")

# Write more data to main so branches diverge further
main_extra = [
    (500, "main_only_event", "2025-06-15 15:00:00", "production"),
]
df_main_extra = spark.createDataFrame(
    main_extra, ["event_id", "event_type", "event_ts_str", "event_source"]
)
df_main_extra = df_main_extra.withColumn(
    "event_ts", F.to_timestamp("event_ts_str")
).drop("event_ts_str")
df_main_extra.writeTo("default.events").overwritePartitions()
print("✅ Overwrote hour 15 partition on main (after feature_branch)")

# ─────────────────────────────────────────────
# 9. Compare all three: main vs audit vs feature
# ─────────────────────────────────────────────
print("\n📋 Main branch:")
spark.table("default.events").show(1000, False)

print("📋 audit_branch:")
spark.read.option("branch", "audit_branch").table("default.events").show(1000, False)

print("📋 feature_branch:")
spark.read.option("branch", "feature_branch").table("default.events").show(1000, False)

spark.sql("DROP TABLE IF EXISTS default.logging")
spark.sql(
    """
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
"""
)
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
df_hour10_new.writeTo("default.logging").option(
    "snapshot-property.my_custom_key",
    "If you have found me, you are searching through IceGraph and doing a great job!",
).overwritePartitions()
print("✅ Overwrote hour 10 partition with corrected data")
print("   (Hour 11 remains untouched)\n")

print("📋 Final table contents:")
spark.table("default.logging").show(1000, False)

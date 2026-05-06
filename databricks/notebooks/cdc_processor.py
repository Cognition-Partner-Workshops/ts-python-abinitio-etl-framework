# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor — Delta Lake MERGE with Change Data Feed
# MAGIC
# MAGIC Migrated from: `graphs/cdc_processor.py` (Ab Initio hash-based CDC pattern)
# MAGIC
# MAGIC ## Ab Initio → Databricks Mapping
# MAGIC | Ab Initio Concept | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | CDCProcessor (row-hash comparison) | Delta Lake MERGE statement |
# MAGIC | MD5 hash for change detection | Delta Change Data Feed (CDF) |
# MAGIC | pandas DataFrame comparison | Spark DataFrame with MERGE INTO |
# MAGIC | Manual INSERT/UPDATE/DELETE routing | MERGE whenMatched / whenNotMatched |
# MAGIC | PSET KEY_COLUMNS / HASH_COLUMNS | Job parameters for merge keys |
# MAGIC
# MAGIC ## Parameters

# COMMAND ----------

# Widget parameters (mapped from customer_cdc.pset)
dbutils.widgets.text("source_path", "/mnt/raw/customer", "Source Snapshot Path")
dbutils.widgets.text("target_table", "catalog.silver.customer_master", "Target Delta Table")
dbutils.widgets.text("key_columns", "customer_id", "Merge Key Columns (comma-separated)")
dbutils.widgets.text("hash_columns", "customer_id,name,address,phone,email,status", "Columns for Change Detection")
dbutils.widgets.text("partition_count", "8", "Spark Partitions")
dbutils.widgets.text("audit_table", "catalog.silver.customer_changes_audit", "Audit Table for CDC Events")
dbutils.widgets.text("retention_days", "90", "Audit Retention (days)")
dbutils.widgets.text("max_errors", "50", "Max Tolerable Errors")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, md5, concat_ws, coalesce, count, when,
    struct, to_json,
)
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_processor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

class CDCConfig:
    """Resolves widget parameters into typed CDC configuration."""

    def __init__(self):
        self.source_path = dbutils.widgets.get("source_path")
        self.target_table = dbutils.widgets.get("target_table")
        self.key_columns = [c.strip() for c in dbutils.widgets.get("key_columns").split(",")]
        self.hash_columns = [c.strip() for c in dbutils.widgets.get("hash_columns").split(",")]
        self.partition_count = int(dbutils.widgets.get("partition_count"))
        self.audit_table = dbutils.widgets.get("audit_table")
        self.retention_days = int(dbutils.widgets.get("retention_days"))
        self.max_errors = int(dbutils.widgets.get("max_errors"))
        self.run_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


config = CDCConfig()
logger.info(
    f"CDC Processor initialized: source={config.source_path}, "
    f"target={config.target_table}, keys={config.key_columns}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Data Reading

# COMMAND ----------

def read_source_snapshot(spark: SparkSession, config: CDCConfig) -> DataFrame:
    """
    Read the current source snapshot.

    In the Ab Initio pattern, this was a full extract from the source system
    that was then compared against the previous snapshot using MD5 hashes.
    """
    logger.info(f"Reading source snapshot from: {config.source_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "|")
        .csv(config.source_path)
    )

    # Repartition for parallel CDC processing
    df = df.repartition(config.partition_count, *[col(k) for k in config.key_columns])

    # Generate row hash for change detection (mirrors Ab Initio _row_hash logic)
    hash_expr = md5(concat_ws("||", *[coalesce(col(c).cast(StringType()), lit("NULL")) for c in config.hash_columns]))
    df = df.withColumn("_row_hash", hash_expr)

    record_count = df.count()
    logger.info(f"Source snapshot: {record_count} records loaded")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake MERGE (CDC Application)

# COMMAND ----------

def apply_cdc_merge(spark: SparkSession, source_df: DataFrame, config: CDCConfig) -> dict:
    """
    Apply CDC changes using Delta Lake MERGE.

    This replaces the Ab Initio CDCProcessor which manually computed
    INSERT/UPDATE/DELETE sets via pandas. Delta MERGE handles this atomically:
      - MATCHED + hash changed → UPDATE
      - NOT MATCHED in target → INSERT
      - NOT MATCHED in source → (optional) DELETE via separate operation

    Delta Change Data Feed (CDF) automatically tracks all mutations for
    downstream consumers, eliminating the need for explicit audit record generation.
    """
    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in config.key_columns]
    )

    # Add metadata columns
    source_df = source_df.withColumn("_updated_at", current_timestamp())
    source_df = source_df.withColumn("_cdc_timestamp", lit(config.run_timestamp))

    if not DeltaTable.isDeltaTable(spark, config.target_table):
        logger.info(f"Target table {config.target_table} does not exist — performing initial full load")
        source_df.drop("_row_hash").write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).saveAsTable(config.target_table)
        return {
            "operation": "initial_load",
            "records_written": source_df.count(),
            "inserts": source_df.count(),
            "updates": 0,
            "deletes": 0,
        }

    delta_table = DeltaTable.forName(spark, config.target_table)

    # Perform MERGE: update changed rows, insert new rows
    merge_result = (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(
            condition="target._row_hash != source._row_hash",
            set={
                c: f"source.{c}" for c in source_df.columns if c != "_row_hash"
            }
        )
        .whenNotMatchedInsert(
            values={
                c: f"source.{c}" for c in source_df.columns if c != "_row_hash"
            }
        )
        .execute()
    )

    # Handle DELETES: records in target but not in source
    target_df = delta_table.toDF()
    source_keys = source_df.select(config.key_columns)
    target_keys = target_df.select(config.key_columns)
    deleted_keys = target_keys.subtract(source_keys)
    delete_count = deleted_keys.count()

    if delete_count > 0:
        logger.info(f"Soft-deleting {delete_count} records no longer in source")
        delete_condition = " AND ".join(
            [f"target.{k} = deletes.{k}" for k in config.key_columns]
        )
        delta_table.alias("target").merge(
            deleted_keys.alias("deletes"), delete_condition
        ).whenMatchedUpdate(set={
            "_is_deleted": lit(True),
            "_deleted_at": current_timestamp(),
        }).execute()

    # Gather metrics from Change Data Feed
    stats = _compute_cdc_stats(spark, config)
    stats["deletes"] = delete_count
    logger.info(f"CDC MERGE completed: {stats}")
    return stats

# COMMAND ----------

def _compute_cdc_stats(spark: SparkSession, config: CDCConfig) -> dict:
    """Read the latest CDF entries to compute insert/update counts."""
    try:
        cdf = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "latest")
            .table(config.target_table)
        )
        inserts = cdf.filter(col("_change_type") == "insert").count()
        updates = cdf.filter(col("_change_type").isin("update_postimage")).count()
        return {"inserts": inserts, "updates": updates, "operation": "merge"}
    except Exception:
        return {"inserts": -1, "updates": -1, "operation": "merge"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Trail

# COMMAND ----------

def write_audit_record(spark: SparkSession, stats: dict, config: CDCConfig):
    """
    Write CDC execution audit record.

    Replaces the Ab Initio audit_customer_changes.mp graph which wrote
    change records to the AUDIT.CUSTOMER_CHANGES table.
    """
    audit_record = spark.createDataFrame([{
        "run_timestamp": config.run_timestamp,
        "target_table": config.target_table,
        "inserts": stats.get("inserts", 0),
        "updates": stats.get("updates", 0),
        "deletes": stats.get("deletes", 0),
        "operation": stats.get("operation", "unknown"),
        "source_path": config.source_path,
        "executed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }])

    audit_record.write.format("delta").mode("append").saveAsTable(config.audit_table)
    logger.info(f"Audit record written to {config.audit_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

try:
    source_df = read_source_snapshot(spark, config)
    stats = apply_cdc_merge(spark, source_df, config)
    write_audit_record(spark, stats, config)

    logger.info(f"CDC Processor completed: {stats}")
    dbutils.notebook.exit(str(stats))

except Exception as e:
    logger.error(f"CDC Processor FAILED: {e}")
    dbutils.notebook.exit(f"FAILED: {e}")
    raise

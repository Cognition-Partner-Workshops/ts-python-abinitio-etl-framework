# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Loader — Partition-Based Ingestion
# MAGIC
# MAGIC Migrated from: `graphs/parallel_loader.py` (Ab Initio partition-based graph execution)
# MAGIC
# MAGIC ## Ab Initio → Databricks Mapping
# MAGIC | Ab Initio Concept | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | PartitionManager (split into N ranges) | Spark repartition / partition pruning |
# MAGIC | air_run per partition (subprocess) | Spark parallel task execution (native) |
# MAGIC | ThreadPoolExecutor orchestration | Spark DAG scheduler (automatic) |
# MAGIC | PSET parameters | Databricks widgets / job parameters |
# MAGIC
# MAGIC ## Parameters

# COMMAND ----------

# Widget parameters (equivalent to Ab Initio PSET values)
dbutils.widgets.text("source_path", "/mnt/raw/orders", "Source Data Path")
dbutils.widgets.text("target_table", "catalog.bronze.order_items", "Target Delta Table")
dbutils.widgets.text("partition_count", "8", "Number of Spark Partitions")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("max_errors", "100", "Max Tolerable Errors")
dbutils.widgets.text("checkpoint_path", "/mnt/checkpoints/parallel_loader", "Checkpoint Location")
dbutils.widgets.dropdown("load_mode", "append", ["append", "overwrite", "merge"], "Load Mode")

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, count, when,
)
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("parallel_loader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

class LoaderConfig:
    """Resolves widget parameters into typed configuration."""

    def __init__(self):
        self.source_path = dbutils.widgets.get("source_path")
        self.target_table = dbutils.widgets.get("target_table")
        self.partition_count = int(dbutils.widgets.get("partition_count"))
        self.batch_date = dbutils.widgets.get("batch_date") or datetime.utcnow().strftime("%Y-%m-%d")
        self.max_errors = int(dbutils.widgets.get("max_errors"))
        self.checkpoint_path = dbutils.widgets.get("checkpoint_path")
        self.load_mode = dbutils.widgets.get("load_mode")


config = LoaderConfig()
logger.info(
    f"Parallel Loader initialized: source={config.source_path}, "
    f"target={config.target_table}, partitions={config.partition_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion with Partition-Based Parallelism

# COMMAND ----------

def read_source_data(spark: SparkSession, config: LoaderConfig) -> DataFrame:
    """
    Read source data with repartitioning for parallel processing.

    In Ab Initio, the PartitionManager split records into ranges and dispatched
    separate air_run processes. In Spark, we achieve the same parallelism via
    repartition() which distributes data across executor tasks.
    """
    logger.info(f"Reading source data from: {config.source_path}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("delimiter", "|")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(config.source_path)
        )
    except AnalysisException as e:
        logger.error(f"Failed to read source path {config.source_path}: {e}")
        raise

    initial_count = df.count()
    logger.info(f"Source records read: {initial_count}")

    # Repartition for parallel processing (equivalent to Ab Initio partition ranges)
    df = df.repartition(config.partition_count)

    # Add metadata columns for lineage
    df = df.withColumn("_ingested_at", current_timestamp())
    df = df.withColumn("_source_file", input_file_name())
    df = df.withColumn("_batch_date", lit(config.batch_date))

    return df

# COMMAND ----------

def validate_data(df: DataFrame, config: LoaderConfig) -> DataFrame:
    """
    Validate ingested data and quarantine bad records.

    Equivalent to Ab Initio's error handling where MAX_ERRORS controls
    whether the graph aborts or continues on bad records.
    """
    error_count = df.filter(col("_corrupt_record").isNotNull()).count()
    logger.info(f"Corrupt/malformed records found: {error_count}")

    if error_count > config.max_errors:
        raise RuntimeError(
            f"Error threshold exceeded: {error_count} errors > max {config.max_errors}. "
            f"Aborting load (equivalent to AI_ERROR_ACTION=ABORT)."
        )

    # Separate clean and error records
    clean_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    return clean_df

# COMMAND ----------

def write_to_delta(df: DataFrame, config: LoaderConfig) -> dict:
    """
    Write validated data to Delta Lake target table.

    Supports three modes mirroring Ab Initio load patterns:
      - append: Simple insert (new partition data)
      - overwrite: Full refresh (replaces all data)
      - merge: Upsert based on key columns (CDC pattern)
    """
    spark = df.sparkSession
    record_count = df.count()
    logger.info(f"Writing {record_count} records to {config.target_table} (mode={config.load_mode})")

    if config.load_mode == "merge":
        if DeltaTable.isDeltaTable(spark, config.target_table):
            delta_table = DeltaTable.forName(spark, config.target_table)
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.order_id = source.order_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df.write.format("delta").mode("overwrite").saveAsTable(config.target_table)
    elif config.load_mode == "overwrite":
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(config.target_table)
    else:
        df.write.format("delta").mode("append").saveAsTable(config.target_table)

    return {
        "records_written": record_count,
        "target_table": config.target_table,
        "load_mode": config.load_mode,
        "batch_date": config.batch_date,
        "partitions_used": config.partition_count,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

try:
    raw_df = read_source_data(spark, config)
    clean_df = validate_data(raw_df, config)
    result = write_to_delta(clean_df, config)

    logger.info(f"Parallel Loader completed successfully: {result}")
    dbutils.notebook.exit(str(result))

except Exception as e:
    logger.error(f"Parallel Loader FAILED: {e}")
    dbutils.notebook.exit(f"FAILED: {e}")
    raise

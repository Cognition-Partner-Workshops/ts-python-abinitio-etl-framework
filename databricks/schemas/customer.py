"""
Delta Lake schema for Customer record.

Source: dml/customer.dml
Type Mapping:
  - decimal(",") customer_id  → LongType (integer identifier, no fractional part)
  - string(",") first_name    → StringType
  - string(",") last_name     → StringType
  - string("\\n") email       → StringType
"""
from pyspark.sql.types import StructType, StructField, LongType, StringType

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
])

CUSTOMER_DDL = """
CREATE TABLE IF NOT EXISTS catalog.bronze.customer (
    customer_id   BIGINT       NOT NULL COMMENT 'Unique customer identifier',
    first_name    STRING       COMMENT 'Customer first name',
    last_name     STRING       COMMENT 'Customer last name',
    email         STRING       COMMENT 'Customer email address',
    _ingested_at  TIMESTAMP    DEFAULT current_timestamp() COMMENT 'Row ingestion timestamp',
    _source_file  STRING       COMMENT 'Source file path for lineage'
)
USING DELTA
PARTITIONED BY (substring(last_name, 1, 1))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Customer master record — migrated from Ab Initio DML customer.dml';
"""

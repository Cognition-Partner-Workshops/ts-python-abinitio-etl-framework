"""
Delta Lake schema for Customer Address record.

Source: dml/customer_address.dml
Type Mapping:
  - decimal(",") customer_id  → LongType
  - string(",") name          → StringType
  - address_t address         → StructType (from common_address.dml)
  - string("\\n") phone       → StringType

Note: The 'include "common_address.dml"' directive is resolved by embedding
the ADDRESS_SCHEMA StructType inline. This is the Delta Lake equivalent of
Ab Initio's type composition via include/type references.
"""
from pyspark.sql.types import StructType, StructField, LongType, StringType

from databricks.schemas.common_address import ADDRESS_SCHEMA

CUSTOMER_ADDRESS_SCHEMA = StructType([
    StructField("customer_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", ADDRESS_SCHEMA, nullable=True),
    StructField("phone", StringType(), nullable=True),
])

CUSTOMER_ADDRESS_DDL = """
CREATE TABLE IF NOT EXISTS catalog.bronze.customer_address (
    customer_id   BIGINT    NOT NULL COMMENT 'FK to customer table',
    name          STRING    COMMENT 'Customer full name',
    address       STRUCT<
        street: STRING,
        city: STRING,
        state: STRING,
        zip: STRING
    >                       COMMENT 'Structured address (from common_address type)',
    phone         STRING    COMMENT 'Contact phone number',
    _ingested_at  TIMESTAMP DEFAULT current_timestamp() COMMENT 'Row ingestion timestamp',
    _source_file  STRING    COMMENT 'Source file path for lineage'
)
USING DELTA
PARTITIONED BY (address.state)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Customer address records with embedded address struct — migrated from Ab Initio DML customer_address.dml';
"""

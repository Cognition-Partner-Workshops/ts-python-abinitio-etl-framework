"""
Delta Lake schema for Order Items record.

Source: dml/order_items.dml
Type Mapping:
  - decimal(",") order_id                   → LongType (integer identifier)
  - decimal(",") item_count                 → IntegerType (count field)
  - string(",")[item_count] item_names      → ArrayType(StringType) (variable-length array)
  - decimal(",")[item_count] item_quantities→ ArrayType(IntegerType) (variable-length array)
  - string("\\n") order_status             → StringType

Note: Ab Initio variable-length arrays (denoted by [field_ref]) are mapped to
Spark ArrayType. The item_count field is retained for backward compatibility but
is redundant given the array's size() function in Spark.
"""
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType, ArrayType,
)

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("item_names", ArrayType(StringType()), nullable=True),
    StructField("item_quantities", ArrayType(IntegerType()), nullable=True),
    StructField("order_status", StringType(), nullable=True),
])

ORDER_ITEMS_DDL = """
CREATE TABLE IF NOT EXISTS catalog.bronze.order_items (
    order_id         BIGINT              NOT NULL COMMENT 'Unique order identifier',
    item_count       INT                 COMMENT 'Number of line items (redundant with array size)',
    item_names       ARRAY<STRING>       COMMENT 'Product names for each line item',
    item_quantities  ARRAY<INT>          COMMENT 'Quantity per line item',
    order_status     STRING              COMMENT 'Current order status',
    _ingested_at     TIMESTAMP           DEFAULT current_timestamp() COMMENT 'Row ingestion timestamp',
    _source_file     STRING              COMMENT 'Source file path for lineage'
)
USING DELTA
PARTITIONED BY (order_status)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Order line items — migrated from Ab Initio DML order_items.dml';
"""

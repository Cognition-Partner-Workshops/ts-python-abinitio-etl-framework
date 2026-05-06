"""
Delta Lake schema for Transaction Detail record.

Source: dml/transaction_detail.dml
Type Mapping:
  - decimal(",") txn_id                          → LongType
  - datetime("YYYY-MM-DD HH24:MI:SS") txn_timestamp → TimestampType
  - decimal(",") customer_id                     → LongType
  - decimal(",") txn_type                        → IntegerType (1=purchase, 2=refund)
  - Nested record merchant_info:
      - string(",", null("")) merchant_name      → StringType (nullable)
      - string(",") merchant_category            → StringType
      - decimal("10.2", ",") amount              → DecimalType(10, 2)
  - decimal(",") item_count                      → IntegerType
  - Nested array record[item_count] line_items:
      - string(",") sku                          → StringType
      - decimal(",") quantity                    → IntegerType
      - decimal("8.2", ",") line_total           → DecimalType(8, 2)
  - Conditional record refund_details (if txn_type == 2):
      - decimal(",") original_txn_id             → LongType (nullable)
      - string(",") refund_reason                → StringType (nullable)
  - string("\\n", null("UNKNOWN")) channel       → StringType (default 'UNKNOWN')

Design Decision: Nested records are mapped to Spark StructType fields.
The conditional refund_details is always present but nullable (null when txn_type != 2).
This avoids schema evolution issues and simplifies downstream queries.
"""
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
    TimestampType, DecimalType, ArrayType,
)

MERCHANT_INFO_SCHEMA = StructType([
    StructField("merchant_name", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("amount", DecimalType(10, 2), nullable=True),
])

LINE_ITEM_SCHEMA = StructType([
    StructField("sku", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("line_total", DecimalType(8, 2), nullable=True),
])

REFUND_DETAILS_SCHEMA = StructType([
    StructField("original_txn_id", LongType(), nullable=True),
    StructField("refund_reason", StringType(), nullable=True),
])

TRANSACTION_DETAIL_SCHEMA = StructType([
    StructField("txn_id", LongType(), nullable=False),
    StructField("txn_timestamp", TimestampType(), nullable=True),
    StructField("customer_id", LongType(), nullable=True),
    StructField("txn_type", IntegerType(), nullable=True),
    StructField("merchant_info", MERCHANT_INFO_SCHEMA, nullable=True),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("line_items", ArrayType(LINE_ITEM_SCHEMA), nullable=True),
    StructField("refund_details", REFUND_DETAILS_SCHEMA, nullable=True),
    StructField("channel", StringType(), nullable=True),
])

TRANSACTION_DETAIL_DDL = """
CREATE TABLE IF NOT EXISTS catalog.bronze.transaction_detail (
    txn_id          BIGINT              NOT NULL COMMENT 'Unique transaction identifier',
    txn_timestamp   TIMESTAMP           COMMENT 'Transaction occurrence timestamp',
    customer_id     BIGINT              COMMENT 'FK to customer table',
    txn_type        INT                 COMMENT 'Transaction type: 1=purchase, 2=refund',
    merchant_info   STRUCT<
        merchant_name: STRING,
        merchant_category: STRING,
        amount: DECIMAL(10, 2)
    >                                   COMMENT 'Merchant details for this transaction',
    item_count      INT                 COMMENT 'Number of line items',
    line_items      ARRAY<STRUCT<
        sku: STRING,
        quantity: INT,
        line_total: DECIMAL(8, 2)
    >>                                  COMMENT 'Individual line items in the transaction',
    refund_details  STRUCT<
        original_txn_id: BIGINT,
        refund_reason: STRING
    >                                   COMMENT 'Refund metadata (null when txn_type != 2)',
    channel         STRING              DEFAULT 'UNKNOWN' COMMENT 'Transaction channel (web, mobile, in-store)',
    _ingested_at    TIMESTAMP           DEFAULT current_timestamp() COMMENT 'Row ingestion timestamp',
    _source_file    STRING              COMMENT 'Source file path for lineage'
)
USING DELTA
PARTITIONED BY (date_trunc('DAY', txn_timestamp))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 30 days'
)
COMMENT 'Transaction details with nested merchant/line-item data — migrated from Ab Initio DML transaction_detail.dml';
"""

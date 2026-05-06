"""
Delta Lake schema for Packed Account record.

Source: dml/packed_account.dml
Type Mapping:
  - packed_decimal(5) account_num   → DecimalType(5, 0) → effectively LongType
  - packed_decimal("7.2") balance   → DecimalType(7, 2)
  - zoned_decimal(4) status_code    → DecimalType(4, 0) → effectively IntegerType
  - string(20) account_name         → StringType (max 20 chars, no truncation in Spark)

Mainframe Format Notes:
  - packed_decimal (COMP-3): Each byte stores two digits except the last nibble
    which holds the sign. A packed_decimal(5) stores up to 5 digits.
  - zoned_decimal (DISPLAY): Each byte stores one digit in the zone/digit format.
    A zoned_decimal(4) stores up to 4 digits.

Migration Strategy:
  During ingestion, raw packed/zoned bytes must be decoded before writing to Delta.
  The PySpark reader should apply a UDF or use the mainframe connector to unpack
  these fields. Once in Delta, they are stored as standard DECIMAL types.
"""
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType, DecimalType,
)

PACKED_ACCOUNT_SCHEMA = StructType([
    StructField("account_num", LongType(), nullable=False),
    StructField("balance", DecimalType(7, 2), nullable=True),
    StructField("status_code", IntegerType(), nullable=True),
    StructField("account_name", StringType(), nullable=True),
])

PACKED_ACCOUNT_DDL = """
CREATE TABLE IF NOT EXISTS catalog.bronze.packed_account (
    account_num   BIGINT         NOT NULL COMMENT 'Account number (decoded from packed_decimal(5))',
    balance       DECIMAL(7, 2)  COMMENT 'Account balance (decoded from packed_decimal(7.2))',
    status_code   INT            COMMENT 'Status code (decoded from zoned_decimal(4))',
    account_name  STRING         COMMENT 'Account holder name (fixed-width 20 chars in source)',
    _ingested_at  TIMESTAMP      DEFAULT current_timestamp() COMMENT 'Row ingestion timestamp',
    _source_file  STRING         COMMENT 'Source file path for lineage'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Packed/zoned decimal account data — migrated from Ab Initio DML packed_account.dml (mainframe format)';
"""

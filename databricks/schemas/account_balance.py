"""
Delta Lake schema for Account Balance record.

Source: dml/account_balance.dml
Type Mapping:
  - decimal("|") account_id         → LongType (integer identifier)
  - string("|") account_holder      → StringType
  - decimal("8.2", "|") balance     → DecimalType(8, 2) (precision 8, scale 2)
  - date("YYYY-MM-DD")(";") opened_date → DateType
  - string("\\n") branch            → StringType
"""
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, DateType,
)

ACCOUNT_BALANCE_SCHEMA = StructType([
    StructField("account_id", LongType(), nullable=False),
    StructField("account_holder", StringType(), nullable=True),
    StructField("balance", DecimalType(8, 2), nullable=True),
    StructField("opened_date", DateType(), nullable=True),
    StructField("branch", StringType(), nullable=True),
])

ACCOUNT_BALANCE_DDL = """
CREATE TABLE IF NOT EXISTS catalog.bronze.account_balance (
    account_id      BIGINT         NOT NULL COMMENT 'Unique account identifier',
    account_holder  STRING         COMMENT 'Name of the account holder',
    balance         DECIMAL(8, 2)  COMMENT 'Current account balance',
    opened_date     DATE           COMMENT 'Date the account was opened',
    branch          STRING         COMMENT 'Branch where account is held',
    _ingested_at    TIMESTAMP      DEFAULT current_timestamp() COMMENT 'Row ingestion timestamp',
    _source_file    STRING         COMMENT 'Source file path for lineage'
)
USING DELTA
PARTITIONED BY (branch)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Account balance records — migrated from Ab Initio DML account_balance.dml';
"""

"""
Delta Lake schema for Account Status record.

Source: dml/account_status.dml
Type Mapping:
  - decimal(",") id         → LongType (integer identifier)
  - void(",") padding1      → (skipped — void fields are Ab Initio filler bytes, no data)
  - string(",") name        → StringType
  - void(",") padding2      → (skipped — void fields are Ab Initio filler bytes, no data)
  - string("\\n") status    → StringType

Note: Ab Initio 'void' fields are used for alignment/padding in fixed-width
record formats. They carry no business data and are dropped during migration.
"""
from pyspark.sql.types import StructType, StructField, LongType, StringType

ACCOUNT_STATUS_SCHEMA = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
])

ACCOUNT_STATUS_DDL = """
CREATE TABLE IF NOT EXISTS catalog.bronze.account_status (
    id            BIGINT    NOT NULL COMMENT 'Account status record identifier',
    name          STRING    COMMENT 'Account holder name',
    status        STRING    COMMENT 'Account status (ACTIVE, CLOSED, SUSPENDED, etc.)',
    _ingested_at  TIMESTAMP DEFAULT current_timestamp() COMMENT 'Row ingestion timestamp',
    _source_file  STRING    COMMENT 'Source file path for lineage'
)
USING DELTA
PARTITIONED BY (status)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Account status records — migrated from Ab Initio DML account_status.dml (void padding fields dropped)';
"""

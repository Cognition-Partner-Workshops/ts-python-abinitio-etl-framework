"""
Delta Lake schema for Common Address type.

Source: dml/common_address.dml
Type Mapping:
  - string(",") street  → StringType
  - string(",") city    → StringType
  - string(",") state   → StringType
  - string(",") zip     → StringType

Note: This is defined as a reusable type (type address_t = record) in Ab Initio.
In Delta Lake, it maps to a StructType that can be embedded in other schemas.
"""
from pyspark.sql.types import StructType, StructField, StringType

ADDRESS_SCHEMA = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
])

COMMON_ADDRESS_DDL = """
-- common_address is a reusable STRUCT type, not a standalone table.
-- Embedded as: address STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>
-- See customer_address table for usage example.
"""

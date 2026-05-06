# Ab Initio → Databricks Lakehouse Migration Runbook

## Executive Summary

This document details the migration of the enterprise Ab Initio ETL estate to Databricks Lakehouse architecture. The migration covers DML schemas, graph execution patterns, KornShell orchestration, and AutoSys monitoring.

---

## 1. Concept Mapping Table

| Ab Initio Concept | Location | Databricks Equivalent | Migrated Location |
|---|---|---|---|
| **Graph (.mp)** | `graphs/` | Databricks Notebook (PySpark) | `databricks/notebooks/` |
| **DML Record Layout** | `dml/*.dml` | PySpark StructType + Delta DDL | `databricks/schemas/` |
| **PSET (Parameter Set)** | `psets/pset_templates/` | Databricks Job Parameters + Widgets | `databricks/workflows/*.json` → `parameters` |
| **Partition (m_partition)** | `graphs/parallel_loader.py` | Spark `repartition()` / native parallelism | `databricks/notebooks/parallel_loader.py` |
| **CDC (Compare Records)** | `graphs/cdc_processor.py` | Delta Lake MERGE + Change Data Feed | `databricks/notebooks/cdc_processor.py` |
| **AutoSys Job** | `scripts/*.ksh` | Databricks Workflow (JSON) | `databricks/workflows/` |
| **KornShell Script** | `scripts/` | Workflow task dependencies | `databricks/workflows/` |
| **setenv.ksh** | `scripts/setenv.ksh` | Cluster spark_conf + job parameters | Workflow `job_clusters[].new_cluster.spark_conf` |
| **air sandbox run** | Shell invocations | `dbutils.notebook.run()` / Workflow tasks | Workflow task definitions |
| **AutoSys Monitoring** | `monitoring/job_monitor.py` | Databricks SQL Alerts | `databricks/monitoring/job_failure_alert.sql` |
| **SLA Tracker** | `monitoring/sla_tracker.py` | Databricks SQL Dashboard | `databricks/monitoring/sla_compliance_dashboard.sql` |
| **Slack Alerts** | `JobMonitor._send_alert()` | Alert notification destinations | `databricks/monitoring/alert_config.json` |
| **Co>Operating System** | Runtime engine | Spark cluster (Databricks Runtime) | Workflow `job_clusters` |
| **Error Handling (ABORT)** | `setenv.ksh: AI_ERROR_ACTION` | `max_errors` parameter + exception handling | Notebook validation logic |
| **Checkpoint/Restart** | `AI_CHECKPOINT_DIR` | Delta transaction log + Structured Streaming checkpoints | `checkpoint_path` parameter |
| **ServiceNow/UrbanCode** | `deployment/` | Databricks Asset Bundles + CI/CD | (out of scope for this PR) |

---

## 2. Type Mapping Reference

### Ab Initio DML → Spark/Delta Types

| Ab Initio DML Type | Example | PySpark Type | Delta SQL Type | Notes |
|---|---|---|---|---|
| `decimal` (no precision) | `decimal(",") id` | `LongType()` | `BIGINT` | Integer identifiers |
| `decimal("P.S")` | `decimal("8.2") balance` | `DecimalType(P, S)` | `DECIMAL(P, S)` | Fixed-point numbers |
| `string` | `string(",") name` | `StringType()` | `STRING` | Variable-length text |
| `string(N)` | `string(20) name` | `StringType()` | `STRING` | Fixed-width in source; no limit in Delta |
| `date("fmt")` | `date("YYYY-MM-DD")` | `DateType()` | `DATE` | Format handled at read time |
| `datetime("fmt")` | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType()` | `TIMESTAMP` | Format handled at read time |
| `packed_decimal(N)` | `packed_decimal(5) acct` | `LongType()` | `BIGINT` | Mainframe COMP-3; decoded during ingestion |
| `packed_decimal("P.S")` | `packed_decimal("7.2") bal` | `DecimalType(P, S)` | `DECIMAL(P, S)` | Mainframe COMP-3 with scale |
| `zoned_decimal(N)` | `zoned_decimal(4) code` | `IntegerType()` | `INT` | Mainframe DISPLAY format; decoded during ingestion |
| `void` | `void(",") padding` | *(dropped)* | *(dropped)* | Filler bytes; no business data |
| Nested `record` | `record ... end name` | `StructType()` | `STRUCT<...>` | Embedded struct |
| `record[N]` (array) | `record[count] items` | `ArrayType(StructType)` | `ARRAY<STRUCT<...>>` | Variable-length array |
| Conditional `if(...)` | `if (type == 2) record` | Nullable `StructType` | Nullable `STRUCT<...>` | Always present, null when condition false |
| `type X = record` | `type address_t = record` | Reusable `StructType` | Reusable `STRUCT<...>` | Compose via Python import |
| `include "file.dml"` | `include "common_address.dml"` | Python `from ... import` | — | Type composition |
| `null("val")` | `string(",", null(""))` | `.nullable=True` | `DEFAULT 'val'` | Null sentinel handling |

---

## 3. Execution Order

### Phase 1: Schema Deployment (Day 1-2)
1. Deploy Unity Catalog namespace: `catalog.bronze`, `catalog.silver`, `catalog.gold`
2. Execute DDL from `databricks/schemas/*.py` to create Delta tables
3. Verify table properties (`enableChangeDataFeed`, `autoOptimize`)

### Phase 2: Notebook Deployment (Day 3-4)
1. Import `databricks/notebooks/` to Databricks Repos
2. Run dry-run tests with sample data from `data/sample/`
3. Validate schema compatibility (StructType matches Delta table)

### Phase 3: Workflow Deployment (Day 5-6)
1. Deploy `databricks/workflows/daily_orders_workflow.json` via Jobs API
2. Deploy `databricks/workflows/customer_cdc_workflow.json` via Jobs API
3. Configure job parameters for `dev` environment
4. Run both workflows manually against test data

### Phase 4: Monitoring Setup (Day 7)
1. Create SQL Alerts from `databricks/monitoring/alert_config.json`
2. Deploy dashboard from `databricks/monitoring/sla_compliance_dashboard.sql`
3. Configure Slack webhook destination
4. Test alert firing with a simulated failure

### Phase 5: Parallel Run (Week 2-3)
1. Run Ab Initio and Databricks pipelines in parallel
2. Compare output record counts and data checksums
3. Validate SLA compliance metrics match
4. Monitor error rates and latency differences

### Phase 6: Cutover (Week 4)
1. Disable AutoSys jobs (JOB_DAILY_ORDERS_LOAD, JOB_CUSTOMER_CDC)
2. Unpause Databricks workflow schedules
3. Monitor first 48 hours of autonomous execution
4. Decommission Ab Initio sandbox access

---

## 4. PSET → Databricks Parameter Mapping

### orders_pipeline.pset → daily_orders_workflow.json

| PSET Parameter | Databricks Mapping | Default (dev) |
|---|---|---|
| `SOURCE_PATH` | `job.parameters.source_path` | `/mnt/raw/orders` |
| `TARGET_TABLE` | Notebook `target_table` widget | `catalog.bronze.orders_raw` |
| `PARTITION_COUNT` | `job.parameters.partition_count` | `8` |
| `BATCH_SIZE` | Spark `spark.sql.shuffle.partitions` | `8` |
| `LOG_LEVEL` | Cluster `spark.log.level` | `INFO` |
| `RECORD_SOURCE` | Lineage metadata column | `_source_file` |
| `MAX_ERRORS` | `job.parameters.max_errors` | `100` |
| `CHECKPOINT_DIR` | Notebook `checkpoint_path` widget | `/mnt/checkpoints/orders/extract` |

### customer_cdc.pset → customer_cdc_workflow.json

| PSET Parameter | Databricks Mapping | Default |
|---|---|---|
| `SOURCE_PATH` | `job.parameters.source_path` | `/mnt/raw/customer` |
| `TARGET_TABLE` | Notebook `target_table` widget | `catalog.silver.customer_master` |
| `PREVIOUS_SNAPSHOT_PATH` | Delta versioning (automatic) | — |
| `CURRENT_SNAPSHOT_PATH` | `catalog.bronze.customer_snapshot_current` | — |
| `CDC_OUTPUT_PATH` | Change Data Feed (automatic) | — |
| `PARTITION_COUNT` | `job.parameters.partition_count` | `8` |
| `HASH_COLUMNS` | `job.parameters.hash_columns` | `customer_id,name,...` |
| `KEY_COLUMNS` | `job.parameters.key_columns` | `customer_id` |
| `BATCH_SIZE` | Spark partition size (automatic) | — |
| `MAX_ERRORS` | `job.parameters.max_errors` | `50` |
| `AUDIT_TABLE` | Notebook `audit_table` widget | `catalog.silver.customer_changes_audit` |
| `RETENTION_DAYS` | `job.parameters.retention_days` | `90` |

---

## 5. Risks and Mitigations

### R1: Packed/Zoned Decimal Handling

| Aspect | Detail |
|---|---|
| **Risk** | Mainframe packed_decimal and zoned_decimal formats require byte-level decoding. Incorrect decoding silently produces wrong numeric values. |
| **Impact** | High — financial data corruption |
| **Mitigation** | Use the Databricks mainframe connector (`spark-mainframe`) or implement a custom UDF that decodes COMP-3/DISPLAY formats. Validate decoded values against known test records from `data/sample/`. |
| **Validation** | Compare decoded values against Oracle source for 1000 randomly sampled `packed_account` records. |

### R2: Partition Strategy Differences

| Aspect | Detail |
|---|---|
| **Risk** | Ab Initio uses fixed record-range partitioning (start/end record numbers). Spark uses hash or range-based partitioning on column values. Load distribution may differ. |
| **Impact** | Medium — potential data skew and performance regression |
| **Mitigation** | Use `repartition(N, col("key"))` for key-based distribution. Monitor partition sizes via Spark UI. Add salting if skew > 3x average. |
| **Validation** | Compare execution times between Ab Initio (4 partitions) and Spark (8 partitions) during parallel run phase. |

### R3: CDC Consistency During Parallel Run

| Aspect | Detail |
|---|---|
| **Risk** | Running CDC in both systems simultaneously may cause conflicts if both attempt to write to the same target (one system sees changes the other already applied). |
| **Impact** | High — data inconsistency |
| **Mitigation** | During parallel run, Databricks writes to a separate `_shadow` table. Comparison is done via count + checksum queries, not by writing to the same target. |
| **Validation** | Daily reconciliation script compares Ab Initio target vs Databricks shadow table. |

### R4: Void Field Handling

| Aspect | Detail |
|---|---|
| **Risk** | `void` fields in Ab Initio DML are alignment padding. If downstream systems depend on field positions (not names), dropping voids changes the column index. |
| **Impact** | Low — only if consumers use positional access |
| **Mitigation** | All Databricks consumers must use named column access. Document the void-to-nothing mapping. Verify no downstream SQL uses `SELECT *` with positional assumptions. |

### R5: Variable-Length Array Semantics

| Aspect | Detail |
|---|---|
| **Risk** | Ab Initio `record[field_ref]` arrays have explicit count fields. Spark ArrayType does not enforce length consistency between the count field and array size. |
| **Impact** | Low — data quality |
| **Mitigation** | Add a CHECK constraint or DQ rule: `size(item_names) == item_count`. Retain `item_count` column for backward compatibility but treat the array `size()` as authoritative. |

### R6: Date/Time Format Parsing

| Aspect | Detail |
|---|---|
| **Risk** | Ab Initio uses `YYYY-MM-DD HH24:MI:SS` format specifiers. Spark uses Java `SimpleDateFormat` patterns (`yyyy-MM-dd HH:mm:ss`). Mismatch causes null values on parse failure. |
| **Impact** | Medium — silent data loss |
| **Mitigation** | Set `spark.sql.legacy.timeParserPolicy=LEGACY` during migration. Add validation counts for null timestamps after ingestion. |

### R7: SLA Window Shift

| Aspect | Detail |
|---|---|
| **Risk** | Databricks cluster startup time (1-3 min) adds latency vs. always-on Ab Initio Co>Op nodes. Jobs may miss tight SLA windows. |
| **Impact** | Medium — SLA breaches during initial period |
| **Mitigation** | Use warm pools or instance pools to reduce startup. Schedule workflows 15 min earlier than Ab Initio. Monitor via `sla_breach_check.sql` alert. |

---

## 6. Environment Configuration Mapping

### setenv.ksh → Databricks Configuration

| Shell Variable | Purpose | Databricks Equivalent |
|---|---|---|
| `AI_HOME` | Ab Initio install path | N/A (managed service) |
| `AI_PROJECT_DIR` | Project root | Databricks Repos path |
| `AI_LOG_DIR` | Log output directory | Driver logs (auto-captured) |
| `AI_DATA_DIR` | Raw data path | `/mnt/raw/` (DBFS mount) |
| `AI_STAGING_DIR` | Staging area | `catalog.silver.*` tables |
| `AI_SOURCE_DB` | Oracle connection | Databricks external connection / JDBC |
| `AI_TARGET_DB` | Teradata DW | Delta tables in `catalog.gold` |
| `AI_DEFAULT_PARTITIONS` | Default parallelism | `spark.sql.shuffle.partitions` |
| `AI_MAX_ERRORS` | Error threshold | `max_errors` job parameter |
| `AI_ERROR_ACTION` | Error behaviour | Exception handling in notebook code |
| `AI_CHECKPOINT_ENABLED` | Checkpoint toggle | Delta transaction log (always on) |
| `AI_CHECKPOINT_DIR` | Checkpoint path | Structured Streaming `checkpointLocation` |

---

## 7. File Inventory

### Delivered Artifacts

| File | Description |
|---|---|
| `databricks/schemas/__init__.py` | Package init |
| `databricks/schemas/customer.py` | Customer schema (StructType + DDL) |
| `databricks/schemas/account_balance.py` | Account Balance schema |
| `databricks/schemas/order_items.py` | Order Items schema (with arrays) |
| `databricks/schemas/transaction_detail.py` | Transaction Detail (nested structs + conditional) |
| `databricks/schemas/packed_account.py` | Packed/Zoned Decimal schema (mainframe) |
| `databricks/schemas/account_status.py` | Account Status (void fields dropped) |
| `databricks/schemas/common_address.py` | Reusable address StructType |
| `databricks/schemas/customer_address.py` | Customer Address (with included type) |
| `databricks/notebooks/__init__.py` | Package init |
| `databricks/notebooks/parallel_loader.py` | Parallel ingestion notebook |
| `databricks/notebooks/cdc_processor.py` | CDC MERGE notebook |
| `databricks/workflows/daily_orders_workflow.json` | Daily orders workflow definition |
| `databricks/workflows/customer_cdc_workflow.json` | Customer CDC workflow (every 4h) |
| `databricks/monitoring/job_failure_alert.sql` | Job failure SQL alert |
| `databricks/monitoring/sla_compliance_dashboard.sql` | SLA dashboard query |
| `databricks/monitoring/sla_breach_check.sql` | SLA breach detection alert |
| `databricks/monitoring/alert_config.json` | Alert configuration manifest |
| `docs/ABINITIO_TO_DATABRICKS_RUNBOOK.md` | This document |

---

## 8. Post-Migration Checklist

- [ ] Unity Catalog namespaces created (`bronze`, `silver`, `gold`)
- [ ] All Delta tables created with `enableChangeDataFeed = true`
- [ ] Notebooks imported to Databricks Repos
- [ ] Workflows deployed and tested with sample data
- [ ] SQL Alerts configured and tested
- [ ] Slack webhook connected
- [ ] Parallel run completed (minimum 7 days)
- [ ] Data reconciliation passed (count + checksum match)
- [ ] SLA compliance ≥ 99% during parallel run
- [ ] AutoSys jobs decommissioned
- [ ] Ab Initio sandbox access revoked
- [ ] Runbook reviewed by data engineering team

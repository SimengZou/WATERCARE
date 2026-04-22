# tirou003

LN tirou003 Task table, Generated 2026-04-10T19:41:49Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tirou003` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `tano` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `tano` | `string` | `varchar` | `PK` | `not_null` | (required) Task. Max length: 8 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `cwoc` | `string` | `varchar` |  |  | Default Work Center. Max length: 6 |
| `mcno` | `string` | `varchar` |  |  | Default Machine. Max length: 6 |
| `mlwc` | `integer` | `int` |  |  | Work Center Selection Method. Allowed values: 1, 2, 3 |
| `mlwc_kw` | `string` | `varchar` |  |  | Work Center Selection Method (keyword). Allowed values: timlwc.default, timlwc.specific, timlwc.all |
| `mlmc` | `integer` | `int` |  |  | Machine Selection Method. Allowed values: 1, 2, 3 |
| `mlmc_kw` | `string` | `varchar` |  |  | Machine Selection Method (keyword). Allowed values: timlwc.default, timlwc.specific, timlwc.all |
| `exin` | `string` | `varchar` |  |  | Extra Information. Max length: 8 |
| `txta` | `integer` | `int` |  |  | Task Text |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `mcno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou002 Machine |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `deleted` | `boolean` | `boolean` |  | `not_null` | (required) Whether record is deleted |
| `username` | `string` | `varchar` |  | `not_null` | (required) User responsible for record action. Max length: 8 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Time the action occurred |
| `sequencenumber` | `integer` | `int` |  | `not_null` | (required) Sequence number of the action |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

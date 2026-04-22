# tppss020

LN tppss020 Baselines table, Generated 2026-04-10T19:42:18Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppss020` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `basn` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `basn` | `integer` | `int` | `PK` | `not_null` | (required) Baseline |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `bsta` | `integer` | `int` |  |  | Baseline Status. Allowed values: 1, 2 |
| `bsta_kw` | `string` | `varchar` |  |  | Baseline Status (keyword). Allowed values: tppss.bsta.free, tppss.bsta.approved |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `apdt` | `timestamp` | `timestamp_ntz` |  |  | Approval Date |
| `lupd` | `timestamp` | `timestamp_ntz` |  |  | Date of Last Update |
| `dbsn` | `integer` | `int` |  |  | Derived from Baseline |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_dbsn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss020 Baselines |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
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

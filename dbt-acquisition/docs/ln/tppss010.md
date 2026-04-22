# tppss010

LN tppss010 Project Plans table, Generated 2026-04-10T19:42:18Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppss010` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cpla` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `plnr` | `string` | `varchar` |  |  | Planner. Max length: 9 |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Start Date |
| `fidt` | `timestamp` | `timestamp_ntz` |  |  | Scheduled Finish Date |
| `tpac` | `string` | `varchar` |  |  | Top Activity. Max length: 16 |
| `plmd` | `integer` | `int` |  |  | Update Mode. Allowed values: 1, 2 |
| `plmd_kw` | `string` | `varchar` |  |  | Update Mode (keyword). Allowed values: tppss.plmd.update, tppss.plmd.read.only |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `plnr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
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

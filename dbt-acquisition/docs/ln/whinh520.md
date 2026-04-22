# whinh520

LN whinh520 Adjustment Orders table, Generated 2026-04-10T19:42:51Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh520` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `adrn` | `string` | `varchar` |  |  | Reason for Adjustment. Max length: 6 |
| `mnad` | `integer` | `int` |  |  | Manual Adjustment. Allowed values: 1, 2 |
| `mnad_kw` | `string` | `varchar` |  |  | Manual Adjustment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adst` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30 |
| `adst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: whinh.adst.open, whinh.adst.modified, whinh.adst.processed |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference Number - base language. Max length: 30 |
| `redt` | `timestamp` | `timestamp_ntz` |  |  | Reference Date |
| `txta` | `integer` | `int` |  |  | Header Text |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `adrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
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

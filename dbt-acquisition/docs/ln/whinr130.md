# whinr130

LN whinr130 Item Issue by Warehouse and Period table, Generated 2025-06-12T01:48:43Z from package combination ce01090

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinr130` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar`, `item`, `year`, `peri` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Year |
| `peri` | `integer` | `int` | `PK` | `not_null` | (required) Period |
| `acip` | `float` | `float` |  |  | Actual Issue by Period |
| `decl` | `float` | `float` |  |  | Demand Calls |
| `deqt` | `float` | `float` |  |  | Demand Quantity |
| `qdeq` | `float` | `float` |  |  | Demand Quantity (Qualified) |
| `lsts` | `float` | `float` |  |  | Lost Sales |
| `dmfp` | `float` | `float` |  |  | Demand Forecast by Period |
| `rets` | `float` | `float` |  |  | Returned Quantity |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
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

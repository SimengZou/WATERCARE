# whina117

LN whina117 Inventory Variance Cost Details table, Generated 2026-04-10T19:42:45Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina117` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `cwar`, `trdt`, `seqn`, `cpcp` |
| **Column count** | 29 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `trdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Date |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `cpcp` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Component. Max length: 8 |
| `hour` | `float` | `float` |  |  | Hours |
| `amnt_1` | `float` | `float` |  |  | Amount |
| `amnt_2` | `float` | `float` |  |  | Amount |
| `amnt_3` | `float` | `float` |  |  | Amount |
| `oamt_1` | `float` | `float` |  |  | Old Amount |
| `oamt_2` | `float` | `float` |  |  | Old Amount |
| `oamt_3` | `float` | `float` |  |  | Old Amount |
| `namt_1` | `float` | `float` |  |  | New Amount |
| `namt_2` | `float` | `float` |  |  | New Amount |
| `namt_3` | `float` | `float` |  |  | New Amount |
| `item_cwar_trdt_seqn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina116 Inventory Variances |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `cpcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
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

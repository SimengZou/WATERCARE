# whwmd217

LN whwmd217 Item Inventory by Warehouse Cost Details table, Generated 2025-06-12T01:48:45Z from package combination ce01090

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whwmd217` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar`, `item`, `cpcp` |
| **Column count** | 25 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cpcp` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Component. Max length: 8 |
| `ftph` | `float` | `float` |  |  | Standard Hours |
| `mauh` | `float` | `float` |  |  | MAUH |
| `ftpa_1` | `float` | `float` |  |  | Standard Costs |
| `ftpa_2` | `float` | `float` |  |  | Standard Costs |
| `ftpa_3` | `float` | `float` |  |  | Standard Costs |
| `mauc_1` | `float` | `float` |  |  | MAUC |
| `mauc_2` | `float` | `float` |  |  | MAUC |
| `mauc_3` | `float` | `float` |  |  | MAUC |
| `cwar_item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd215 Item Inventory by Warehouse |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
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

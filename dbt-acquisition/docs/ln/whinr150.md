# whinr150

LN whinr150 Inventory Structure table, Generated 2025-06-12T01:48:44Z from package combination ce01090

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinr150` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `cwar`, `loca`, `clot`, `idat`, `pkdf`, `levl`, `cuni` |
| **Column count** | 30 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` | `PK` | `not_null` | (required) Location. Max length: 10 |
| `clot` | `string` | `varchar` | `PK` | `not_null` | (required) Lot. Max length: 20 |
| `idat` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Inventory Date |
| `pkdf` | `string` | `varchar` | `PK` | `not_null` | (required) Package Definition. Max length: 6 |
| `levl` | `integer` | `int` | `PK` | `not_null` | (required) Level |
| `cuni` | `string` | `varchar` | `PK` | `not_null` | (required) Unit. Max length: 3 |
| `qhds` | `float` | `float` |  |  | Inventory on Hand in Storage Unit |
| `qhnd` | `float` | `float` |  |  | Inventory on Hand |
| `prbl` | `float` | `float` |  |  | Process Blocked Quantity in Storage Unit |
| `mabl` | `float` | `float` |  |  | Manually Blocked Quantity in Storage Unit |
| `mabp` | `float` | `float` |  |  | Manually Blocked Quantity for Planning |
| `qals` | `float` | `float` |  |  | Allocated Quantity in Storage Unit |
| `qcom` | `float` | `float` |  |  | Inventory Committed |
| `qcsl` | `float` | `float` |  |  | Inventory Committed Specific Lot |
| `qavs` | `float` | `float` |  |  | Available Quantity in Storage Unit |
| `qavl` | `float` | `float` |  |  | Available Quantity in Inventory Unit |
| `item_pkdf_levl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd440 Package Definition Levels by Item |
| `cwar_loca_item_clot_idat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinr140 Inventory |
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

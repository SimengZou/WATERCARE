# whinr140

LN whinr140 Inventory table, Generated 2025-06-12T01:48:44Z from package combination ce01090

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinr140` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar`, `loca`, `item`, `clot`, `idat` |
| **Column count** | 43 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` | `PK` | `not_null` | (required) Location. Max length: 10 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `clot` | `string` | `varchar` | `PK` | `not_null` | (required) Lot. Max length: 20 |
| `idat` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Inventory Date |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `qhnd` | `float` | `float` |  |  | Inventory on Hand |
| `qblk` | `float` | `float` |  |  | Inventory Blocked |
| `qlal` | `float` | `float` |  |  | Inventory Allocated |
| `qord` | `float` | `float` |  |  | Inventory on Order |
| `qcom` | `float` | `float` |  |  | Inventory Committed |
| `qcsl` | `float` | `float` |  |  | Inventory Committed Specific Lot |
| `ball` | `integer` | `int` |  |  | Blocked for All Transactions. Allowed values: 1, 2 |
| `ball_kw` | `string` | `varchar` |  |  | Blocked for All Transactions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bout` | `integer` | `int` |  |  | Blocked for Outbound. Allowed values: 1, 2 |
| `bout_kw` | `string` | `varchar` |  |  | Blocked for Outbound (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `btri` | `integer` | `int` |  |  | Blocked for Transfer (Issue). Allowed values: 1, 2 |
| `btri_kw` | `string` | `varchar` |  |  | Blocked for Transfer (Issue) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bcyc` | `integer` | `int` |  |  | Blocked for Cycle Counting. Allowed values: 1, 2 |
| `bcyc_kw` | `string` | `varchar` |  |  | Blocked for Cycle Counting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bass` | `integer` | `int` |  |  | Blocked for Assembly. Allowed values: 1, 2 |
| `bass_kw` | `string` | `varchar` |  |  | Blocked for Assembly (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crem` | `string` | `varchar` |  |  | Remark. Max length: 3 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `trdt` | `timestamp` | `timestamp_ntz` |  |  | Last Transaction Date |
| `lsid` | `timestamp` | `timestamp_ntz` |  |  | Last Inventory Inspection Date |
| `mpkd` | `integer` | `int` |  |  | Multiple Package Definition. Allowed values: 1, 2 |
| `mpkd_kw` | `string` | `varchar` |  |  | Multiple Package Definition (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cwar_loca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `crem_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd130 Remarks |
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

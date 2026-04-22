# whwmd215

LN whwmd215 Item Inventory by Warehouse table, Generated 2026-04-10T19:42:53Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whwmd215` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar`, `item` |
| **Column count** | 45 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `qhnd` | `float` | `float` |  |  | Inventory on Hand |
| `qchd` | `float` | `float` |  |  | Consignment Inventory on Hand |
| `qnhd` | `float` | `float` |  |  | Customer Owned Inventory on Hand |
| `qblk` | `float` | `float` |  |  | Inventory Blocked |
| `qbpl` | `float` | `float` |  |  | Inventory Blocked for Planning |
| `qord` | `float` | `float` |  |  | Inventory on Order |
| `qoor` | `float` | `float` |  |  | Company Owned Inventory on Order |
| `qcor` | `float` | `float` |  |  | Consignment Inventory on Order |
| `qnor` | `float` | `float` |  |  | Customer Owned Inventory on Order |
| `qgit` | `float` | `float` |  |  | Goods in Transit |
| `qint` | `float` | `float` |  |  | Inventory in Transit |
| `qcit` | `float` | `float` |  |  | Consignment Inventory in Transit |
| `qnit` | `float` | `float` |  |  | Customer Owned Inventory in Transit |
| `qall` | `float` | `float` |  |  | Inventory Allocated |
| `qoal` | `float` | `float` |  |  | Company Owned Inventory Location Allocated |
| `qcal` | `float` | `float` |  |  | Consignment Inventory Location Allocated |
| `qnal` | `float` | `float` |  |  | Customer Owned Inventory Location Allocated |
| `qnbl` | `float` | `float` |  |  | Customer Owned Inventory Blocked |
| `qnbp` | `float` | `float` |  |  | Customer Owned Inventory Blocked for Planning |
| `qcom` | `float` | `float` |  |  | Inventory Committed |
| `qlal` | `float` | `float` |  |  | Inventory Location Allocated |
| `qcpr` | `float` | `float` |  |  | Inventory Committed in Process |
| `qhrj` | `float` | `float` |  |  | Company Owned Quarantine Inventory |
| `qcrj` | `float` | `float` |  |  | Consigned Quarantine Inventory |
| `qnrj` | `float` | `float` |  |  | Customer Owned Quarantine Inventory |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Last Inventory Transaction Date |
| `hstd` | `timestamp` | `timestamp_ntz` |  |  | Historical Inventory Balance Date |
| `lsid` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `qcis` | `float` | `float` |  |  | Cumulative Issue |
| `qhib` | `float` | `float` |  |  | Historical Inventory Balance |
| `cwar_item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd210 Item Data by Warehouse |
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

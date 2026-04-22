# whinh540

LN whinh540 Cycle Counting Data table, Generated 2026-04-10T19:42:52Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh540` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar`, `loca`, `item`, `clot`, `date` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` | `PK` | `not_null` | (required) Location. Max length: 10 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `clot` | `string` | `varchar` | `PK` | `not_null` | (required) Lot. Max length: 20 |
| `date` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Inventory Date |
| `lcdt` | `timestamp` | `timestamp_ntz` |  |  | Last Counting Date |
| `fcnt` | `integer` | `int` |  |  | Force Cycle Count. Allowed values: 1, 2 |
| `fcnt_kw` | `string` | `varchar` |  |  | Force Cycle Count (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `cntn` | `integer` | `int` |  |  | Count Number |
| `pono` | `integer` | `int` |  |  | Line |
| `cwar_loca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `item_clot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whltc100 Lots |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `orno_cntn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh500 Cycle Counting Orders |
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

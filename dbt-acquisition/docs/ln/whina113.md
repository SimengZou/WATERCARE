# whina113

LN whina113 Inventory Receipt Transaction - Cost Details table, Generated 2026-04-10T19:42:44Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina113` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `cwar`, `trdt`, `seqn`, `inwp`, `cpcp` |
| **Column count** | 41 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `trdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Date |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `inwp` | `integer` | `int` | `PK` | `not_null` | (required) Inventory WIP. Allowed values: 1, 2 |
| `inwp_kw` | `string` | `varchar` |  |  | Inventory WIP (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpcp` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Component. Max length: 8 |
| `hour` | `float` | `float` |  |  | Hours |
| `mauh` | `float` | `float` |  |  | MAUH |
| `math` | `float` | `float` |  |  | Total MAUH |
| `amah` | `float` | `float` |  |  | MAUH (by Warehouse and Attribute Set) |
| `atmh` | `float` | `float` |  |  | Total MAUH (by Warehouse and Attribute Set) |
| `amnt_1` | `float` | `float` |  |  | Amount |
| `amnt_2` | `float` | `float` |  |  | Amount |
| `amnt_3` | `float` | `float` |  |  | Amount |
| `mauc_1` | `float` | `float` |  |  | MAUC |
| `mauc_2` | `float` | `float` |  |  | MAUC |
| `mauc_3` | `float` | `float` |  |  | MAUC |
| `matc_1` | `float` | `float` |  |  | Total MAUC |
| `matc_2` | `float` | `float` |  |  | Total MAUC |
| `matc_3` | `float` | `float` |  |  | Total MAUC |
| `amac_1` | `float` | `float` |  |  | MAUC (by Warehouse and Attribute Set) |
| `amac_2` | `float` | `float` |  |  | MAUC (by Warehouse and Attribute Set) |
| `amac_3` | `float` | `float` |  |  | MAUC (by Warehouse and Attribute Set) |
| `atmc_1` | `float` | `float` |  |  | Total MAUC (by Warehouse and Attribute Set) |
| `atmc_2` | `float` | `float` |  |  | Total MAUC (by Warehouse and Attribute Set) |
| `atmc_3` | `float` | `float` |  |  | Total MAUC (by Warehouse and Attribute Set) |
| `item_cwar_trdt_seqn_inwp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina112 Inventory Receipt Transactions |
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

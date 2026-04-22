# whina122

LN whina122 Inventory Revaluation Transactions table, Generated 2026-04-10T19:42:46Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina122` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `rorg`, `orno`, `seqn` |
| **Column count** | 68 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `rorg` | `integer` | `int` | `PK` | `not_null` | (required) Revaluation Origin. Allowed values: 10, 20, 30, 35, 40, 50, 100 |
| `rorg_kw` | `string` | `varchar` |  |  | Revaluation Origin (keyword). Allowed values: whina.rorg.act.cost.price, whina.rorg.antedating, whina.rorg.mauc.correct, whina.rorg.act.cost.corr, whina.rorg.change.val.meth, whina.rorg.change.int.fam, whina.rorg.not.appl |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Revaluation Order. Max length: 9 |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `rvrs` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `rvdt` | `timestamp` | `timestamp_ntz` |  |  | Revaluation Date |
| `lgdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Log Date |
| `logn` | `string` | `varchar` |  |  | User. Max length: 16 |
| `quan` | `float` | `float` |  |  | Quantity |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `tagn` | `string` | `varchar` |  |  | Tag. Max length: 9 |
| `inwp` | `integer` | `int` |  |  | Inventory WIP. Allowed values: 1, 2 |
| `inwp_kw` | `string` | `varchar` |  |  | Inventory WIP (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ovba` | `integer` | `int` |  |  | Old Valuation by Attribute Set. Allowed values: 1, 2 |
| `ovba_kw` | `string` | `varchar` |  |  | Old Valuation by Attribute Set (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oval` | `integer` | `int` |  |  | Old Valuation Method. Allowed values: 1, 2, 3, 4, 5, 6, 20, 255 |
| `oval_kw` | `string` | `varchar` |  |  | Old Valuation Method (keyword). Allowed values: whina.valm.ftp, whina.valm.mauc, whina.valm.fifo, whina.valm.lifo, whina.valm.lot, whina.valm.serial, whina.valm.tag, whina.valm.not.appl |
| `owvg` | `integer` | `int` |  |  | Old Valuation by Warehouse Valuation Group. Allowed values: 1, 2 |
| `owvg_kw` | `string` | `varchar` |  |  | Old Valuation by Warehouse Valuation Group (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ovgr` | `string` | `varchar` |  |  | Old Warehouse Valuation Group. Max length: 6 |
| `nvba` | `integer` | `int` |  |  | New Valuation by Attribute Set. Allowed values: 1, 2 |
| `nvba_kw` | `string` | `varchar` |  |  | New Valuation by Attribute Set (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nval` | `integer` | `int` |  |  | New Valuation Method. Allowed values: 1, 2, 3, 4, 5, 6, 20, 255 |
| `nval_kw` | `string` | `varchar` |  |  | New Valuation Method (keyword). Allowed values: whina.valm.ftp, whina.valm.mauc, whina.valm.fifo, whina.valm.lifo, whina.valm.lot, whina.valm.serial, whina.valm.tag, whina.valm.not.appl |
| `nwvg` | `integer` | `int` |  |  | New Valuation by Warehouse Valuation Group. Allowed values: 1, 2 |
| `nwvg_kw` | `string` | `varchar` |  |  | New Valuation by Warehouse Valuation Group (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nvgr` | `string` | `varchar` |  |  | New Warehouse Valuation Group. Max length: 6 |
| `cvmo` | `integer` | `int` |  |  | Use Old Valuation Method. Allowed values: 1, 2 |
| `cvmo_kw` | `string` | `varchar` |  |  | Use Old Valuation Method (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `reje` | `integer` | `int` |  |  | Quarantine Inventory. Allowed values: 1, 2 |
| `reje_kw` | `string` | `varchar` |  |  | Quarantine Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `rvrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ovgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina102 Warehouse Valuation Groups |
| `nvgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina102 Warehouse Valuation Groups |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
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

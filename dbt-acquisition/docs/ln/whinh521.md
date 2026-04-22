# whinh521

LN whinh521 Adjustment Order Lines table, Generated 2026-04-10T19:42:52Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh521` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `pono` |
| **Column count** | 76 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `loca` | `string` | `varchar` |  |  | Location. Max length: 10 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `itag` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `idat` | `timestamp` | `timestamp_ntz` |  |  | Inventory Date |
| `stun` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `pkdf` | `string` | `varchar` |  |  | Package Definition. Max length: 6 |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `hupr` | `integer` | `int` |  |  | Handling Unit Present. Allowed values: 1, 2 |
| `hupr_kw` | `string` | `varchar` |  |  | Handling Unit Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qstp` | `float` | `float` |  |  | Stock Point Inventory (Inventory Unit) |
| `qstr` | `float` | `float` |  |  | Stock Point Inventory (Storage Unit) |
| `qspu` | `float` | `float` |  |  | Stock Point Inventory (Piece Unit) |
| `qadj` | `float` | `float` |  |  | Inventory Adjusted (Inventory Unit) |
| `qadr` | `float` | `float` |  |  | Inventory Adjusted (Storage Unit) |
| `qapu` | `float` | `float` |  |  | Inventory Adjusted (Piece Unit) |
| `qvrc` | `float` | `float` |  |  | Variance (Inventory Unit) |
| `qvrr` | `float` | `float` |  |  | Variance (Storage Unit) |
| `qvpu` | `float` | `float` |  |  | Variance (Piece Unit) |
| `rjin` | `integer` | `int` |  |  | Quarantine Inventory. Allowed values: 1, 2 |
| `rjin_kw` | `string` | `varchar` |  |  | Quarantine Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adat` | `timestamp` | `timestamp_ntz` |  |  | Adjustment Date |
| `appr` | `integer` | `int` |  |  | Approval. Allowed values: 1, 2, 3, 4 |
| `appr_kw` | `string` | `varchar` |  |  | Approval (keyword). Allowed values: whinh.vara.to.approve, whinh.vara.approved, whinh.vara.recount, whinh.vara.not.required |
| `adrn` | `string` | `varchar` |  |  | Reason for Adjustment. Max length: 6 |
| `uapr` | `integer` | `int` |  |  | Use Adjustment Price. Allowed values: 1, 2 |
| `uapr_kw` | `string` | `varchar` |  |  | Use Adjustment Price (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adpr` | `float` | `float` |  |  | Adjustment Price |
| `pric` | `float` | `float` |  |  | Inventory Value |
| `amnt` | `float` | `float` |  |  | Transaction Amount |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `istr` | `integer` | `int` |  |  | Issue Strategy. Allowed values: 10, 20, 30, 90 |
| `istr_kw` | `string` | `varchar` |  |  | Issue Strategy (keyword). Allowed values: tcistr.free, tcistr.preferred, tcistr.restricted, tcistr.not.appl |
| `ifbp` | `string` | `varchar` |  |  | Issue from Business Partner. Max length: 9 |
| `iown` | `integer` | `int` |  |  | Issue Ownership. Allowed values: 10, 20, 30, 40, 90 |
| `iown_kw` | `string` | `varchar` |  |  | Issue Ownership (keyword). Allowed values: tciown.default, tciown.comp.owned, tciown.consigned, tciown.cust.owned, tciown.not.appl |
| `dpby` | `integer` | `int` |  |  | Inventory Discrepancies Paid By. Allowed values: 10, 20, 30, 90 |
| `dpby_kw` | `string` | `varchar` |  |  | Inventory Discrepancies Paid By (keyword). Allowed values: whdpby.terms.and.cond, whdpby.own.comp, whdpby.owner, whdpby.not.appl |
| `prcd` | `integer` | `int` |  |  | Processed. Allowed values: 1, 2 |
| `prcd_kw` | `string` | `varchar` |  |  | Processed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Line Text |
| `orno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh520 Adjustment Orders |
| `cwar_loca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `item_clot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whltc100 Lots |
| `item_atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd031 Item - Attribute Sets |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `stun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `pkdf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd410 Package Definitions |
| `huid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd530 Handling Units |
| `adrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
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

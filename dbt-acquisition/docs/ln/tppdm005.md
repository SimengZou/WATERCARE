# tppdm005

LN tppdm005 Item Project Data table, Generated 2026-04-10T19:41:53Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm005` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item` |
| **Column count** | 41 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `ccit` | `string` | `varchar` |  |  | Control Code Material. Max length: 47 |
| `ccfu` | `integer` | `int` |  |  | Control Function. Allowed values: 1, 2, 3 |
| `ccfu_kw` | `string` | `varchar` |  |  | Control Function (keyword). Allowed values: tppdm.ccfu.c.unit, tppdm.ccfu.c.unit.cc.code, tppdm.ccfu.cc.code |
| `prre` | `integer` | `int` |  |  | Register Progress. Allowed values: 1, 2 |
| `prre_kw` | `string` | `varchar` |  |  | Register Progress (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `osys` | `integer` | `int` |  |  | Project Order System. Allowed values: 3, 5, 7, 9 |
| `osys_kw` | `string` | `varchar` |  |  | Project Order System (keyword). Allowed values: tppdm.osyb.prp, tppdm.osyb.prp.purchase, tppdm.osyb.prp.warehouse, tppdm.osyb.mnl |
| `pgwh` | `integer` | `int` |  |  | Peg PRP Warehouse Order. Allowed values: 1, 2 |
| `pgwh_kw` | `string` | `varchar` |  |  | Peg PRP Warehouse Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `copt` | `integer` | `int` |  |  | Standard Cost Type. Allowed values: 1, 2, 10 |
| `copt_kw` | `string` | `varchar` |  |  | Standard Cost Type (keyword). Allowed values: tppdm.copt.manu, tppdm.copt.stock, tppdm.copt.rocr |
| `cprp` | `float` | `float` |  |  | Manual Unit Cost |
| `ccur` | `string` | `varchar` |  |  | Unit Cost Currency. Max length: 3 |
| `ltcp` | `timestamp` | `timestamp_ntz` |  |  | Unit Cost Transaction Date |
| `cppp` | `integer` | `int` |  |  | Price Policy. Allowed values: 1, 2 |
| `cppp_kw` | `string` | `varchar` |  |  | Price Policy (keyword). Allowed values: tppdm.cppp.cost.price, tppdm.cppp.purchase.price |
| `cpmc_1` | `float` | `float` |  |  | Manual Unit Cost Home Currencies |
| `cpmc_2` | `float` | `float` |  |  | Manual Unit Cost Home Currencies |
| `cpmc_3` | `float` | `float` |  |  | Manual Unit Cost Home Currencies |
| `usyn` | `integer` | `int` |  |  | Used in Schedule. Allowed values: 1, 2 |
| `usyn_kw` | `string` | `varchar` |  |  | Used in Schedule (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Item Text |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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

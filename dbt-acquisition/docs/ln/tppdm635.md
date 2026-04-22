# tppdm635

LN tppdm635 Project Subcontracting table, Generated 2026-04-10T19:42:01Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm635` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `csub` |
| **Column count** | 105 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `csub` | `string` | `varchar` | `PK` | `not_null` | (required) Subcontracting. Max length: 47 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `dfsb` | `string` | `varchar` |  |  | Derived from. Max length: 47 |
| `citg` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `csel` | `string` | `varchar` |  |  | Selection Code. Max length: 3 |
| `csig` | `string` | `varchar` |  |  | Item Signal. Max length: 3 |
| `uset` | `string` | `varchar` |  |  | Unit Set. Max length: 6 |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `cccp` | `string` | `varchar` |  |  | Unit Cost Currency. Max length: 3 |
| `prip` | `float` | `float` |  |  | Purchase Price |
| `ccur` | `string` | `varchar` |  |  | Purchase Price Currency. Max length: 3 |
| `pris` | `float` | `float` |  |  | Sales Price |
| `ccsp` | `string` | `varchar` |  |  | Sales Price Currency. Max length: 3 |
| `csgp` | `string` | `varchar` |  |  | Purchase Statistics Group. Max length: 6 |
| `csgs` | `string` | `varchar` |  |  | Sales Statistics Group. Max length: 6 |
| `dfpc` | `integer` | `int` |  |  | Final Unit Cost. Allowed values: 1, 2 |
| `dfpc_kw` | `string` | `varchar` |  |  | Final Unit Cost (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `dfpp` | `integer` | `int` |  |  | Final Purchase Price. Allowed values: 1, 2 |
| `dfpp_kw` | `string` | `varchar` |  |  | Final Purchase Price (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `dfps` | `integer` | `int` |  |  | Final Sales Amount. Allowed values: 1, 2 |
| `dfps_kw` | `string` | `varchar` |  |  | Final Sales Amount (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `ltpc` | `timestamp` | `timestamp_ntz` |  |  | Unit Cost Transaction Date |
| `ltpp` | `timestamp` | `timestamp_ntz` |  |  | Purch. Price Trans. Date |
| `ccsb` | `string` | `varchar` |  |  | Control Code Subcontracting. Max length: 47 |
| `ccfu` | `integer` | `int` |  |  | Control Function. Allowed values: 1, 2, 3 |
| `ccfu_kw` | `string` | `varchar` |  |  | Control Function (keyword). Allowed values: tppdm.ccfu.c.unit, tppdm.ccfu.c.unit.cc.code, tppdm.ccfu.cc.code |
| `ltps` | `timestamp` | `timestamp_ntz` |  |  | Sales Price Transaction Date |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `pwgc` | `float` | `float` |  |  | Labor Percentage |
| `dsbc` | `integer` | `int` |  |  | Cost Determination. Allowed values: 1, 2, 10, 20 |
| `dsbc_kw` | `string` | `varchar` |  |  | Cost Determination (keyword). Allowed values: tppdm.dsbc.quantity, tppdm.dsbc.time, tppdm.dsbc.amount.only, tppdm.dsbc.rate.only |
| `buni` | `integer` | `int` |  |  | Base Unit. Allowed values: 10, 20, 30 |
| `buni_kw` | `string` | `varchar` |  |  | Base Unit (keyword). Allowed values: tppdm.qty.time.not.appl, tppdm.qty.time.quantity, tppdm.qty.time.time |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `prby` | `integer` | `int` |  |  | Procurement by. Allowed values: 10, 20, 30 |
| `prby_kw` | `string` | `varchar` |  |  | Procurement by (keyword). Allowed values: tppdm.prby.po, tppdm.prby.sub.po, tppdm.prby.eqp.po |
| `srtr` | `string` | `varchar` |  |  | Services Trade. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `osys` | `integer` | `int` |  |  | Order System. Allowed values: 3, 5, 7, 9 |
| `osys_kw` | `string` | `varchar` |  |  | Order System (keyword). Allowed values: tppdm.osyb.prp, tppdm.osyb.prp.purchase, tppdm.osyb.prp.warehouse, tppdm.osyb.mnl |
| `prre` | `integer` | `int` |  |  | Register Progress. Allowed values: 1, 2 |
| `prre_kw` | `string` | `varchar` |  |  | Register Progress (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cpgp` | `string` | `varchar` |  |  | Purchase Price Group. Max length: 6 |
| `cpgs` | `string` | `varchar` |  |  | Sales Price Group. Max length: 6 |
| `cppp` | `integer` | `int` |  |  | Price Policy. Allowed values: 1, 2 |
| `cppp_kw` | `string` | `varchar` |  |  | Price Policy (keyword). Allowed values: tppdm.cppp.cost.price, tppdm.cppp.purchase.price |
| `oltm` | `integer` | `int` |  |  | Order Lead Time |
| `pcmc_1` | `float` | `float` |  |  | Unit Cost Multi Currency |
| `pcmc_2` | `float` | `float` |  |  | Unit Cost Multi Currency |
| `pcmc_3` | `float` | `float` |  |  | Unit Cost Multi Currency |
| `ppmc_1` | `float` | `float` |  |  | Purchase Price Multi Currency |
| `ppmc_2` | `float` | `float` |  |  | Purchase Price Multi Currency |
| `ppmc_3` | `float` | `float` |  |  | Purchase Price Multi Currency |
| `psmc_1` | `float` | `float` |  |  | Sales Price Multi Currency |
| `psmc_2` | `float` | `float` |  |  | Sales Price Multi Currency |
| `psmc_3` | `float` | `float` |  |  | Sales Price Multi Currency |
| `svat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cood` | `string` | `varchar` |  |  | Technical Coordinator. Max length: 9 |
| `ccde` | `string` | `varchar` |  |  | Harmonized System Code. Max length: 25 |
| `rdpt` | `string` | `varchar` |  |  | Responsible Department. Max length: 6 |
| `plid` | `string` | `varchar` |  |  | Planner. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `dfsb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm035 Subcontracting |
| `citg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs023 Item Groups |
| `csel_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs022 Selection Codes |
| `csig_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs018 Item Signals |
| `uset_cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs012 Units by Unit Set |
| `uset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs006 Unit Sets |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cccp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccsp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `csgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs044 Statistical Groups |
| `csgs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs044 Statistical Groups |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `srtr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs213 Services Trades |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `cpgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `cpgs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `svat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `cood_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ccde_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs028 Harmonized System Codes |
| `rdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `plid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `csub_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Equipment |
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

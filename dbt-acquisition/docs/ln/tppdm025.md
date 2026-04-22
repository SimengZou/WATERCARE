# tppdm025

LN tppdm025 Equipment table, Generated 2026-04-10T19:41:54Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm025` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cequ` |
| **Column count** | 138 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cequ` | `string` | `varchar` | `PK` | `not_null` | (required) Equipment. Max length: 47 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `ceqg` | `string` | `varchar` |  |  | Equipment Group. Max length: 47 |
| `citg` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `csel` | `string` | `varchar` |  |  | Selection Code. Max length: 3 |
| `csig` | `string` | `varchar` |  |  | Item Signal. Max length: 3 |
| `uset` | `string` | `varchar` |  |  | Unit Set. Max length: 6 |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `ratc` | `float` | `float` |  |  | Cost Rate |
| `cccr` | `string` | `varchar` |  |  | Cost Rate Currency. Max length: 3 |
| `cpgp` | `string` | `varchar` |  |  | Purchase Price Group. Max length: 6 |
| `csgp` | `string` | `varchar` |  |  | Purchase Statistics Group. Max length: 6 |
| `ccur` | `string` | `varchar` |  |  | Purchase Rate Currency. Max length: 3 |
| `ratp` | `float` | `float` |  |  | Purchase Rate |
| `cpgs` | `string` | `varchar` |  |  | Sales Price Group. Max length: 6 |
| `csgs` | `string` | `varchar` |  |  | Sales Statistics Group. Max length: 6 |
| `rats` | `float` | `float` |  |  | Sales Rate |
| `ccsr` | `string` | `varchar` |  |  | Sales Rate Currency. Max length: 3 |
| `ltrc` | `timestamp` | `timestamp_ntz` |  |  | Cost Rate Transaction Date |
| `ltrs` | `timestamp` | `timestamp_ntz` |  |  | Sales Rate Transaction Date |
| `ltrp` | `timestamp` | `timestamp_ntz` |  |  | Purchase Rate Trans. Date |
| `cceq` | `string` | `varchar` |  |  | Control Code Equipment. Max length: 47 |
| `ccfu` | `integer` | `int` |  |  | Control Function. Allowed values: 1, 2, 3 |
| `ccfu_kw` | `string` | `varchar` |  |  | Control Function (keyword). Allowed values: tppdm.ccfu.c.unit, tppdm.ccfu.c.unit.cc.code, tppdm.ccfu.cc.code |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `unav` | `float` | `float` |  |  | Number of Units Available |
| `dcpu` | `float` | `float` |  |  | Normal Cap. Utilization by Day |
| `exeq` | `integer` | `int` |  |  | External Equipment. Allowed values: 1, 2 |
| `exeq_kw` | `string` | `varchar` |  |  | External Equipment (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `osys` | `integer` | `int` |  |  | Order System. Allowed values: 3, 5, 7, 9 |
| `osys_kw` | `string` | `varchar` |  |  | Order System (keyword). Allowed values: tppdm.osyb.prp, tppdm.osyb.prp.purchase, tppdm.osyb.prp.warehouse, tppdm.osyb.mnl |
| `dsbc` | `integer` | `int` |  |  | Cost Determination. Allowed values: 1, 2, 10, 20 |
| `dsbc_kw` | `string` | `varchar` |  |  | Cost Determination (keyword). Allowed values: tppdm.dsbc.quantity, tppdm.dsbc.time, tppdm.dsbc.amount.only, tppdm.dsbc.rate.only |
| `prby` | `integer` | `int` |  |  | Procurement by. Allowed values: 10, 20, 30 |
| `prby_kw` | `string` | `varchar` |  |  | Procurement by (keyword). Allowed values: tppdm.prby.po, tppdm.prby.sub.po, tppdm.prby.eqp.po |
| `srtr` | `string` | `varchar` |  |  | Services Trade. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `prre` | `integer` | `int` |  |  | Register Progress. Allowed values: 1, 2 |
| `prre_kw` | `string` | `varchar` |  |  | Register Progress (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `ccal` | `string` | `varchar` |  |  | Calendar. Max length: 9 |
| `avtp` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `cppp` | `integer` | `int` |  |  | Price Policy. Allowed values: 1, 2 |
| `cppp_kw` | `string` | `varchar` |  |  | Price Policy (keyword). Allowed values: tppdm.cppp.cost.price, tppdm.cppp.purchase.price |
| `oltm` | `integer` | `int` |  |  | Order Lead Time |
| `usyn` | `integer` | `int` |  |  | Used in Schedule. Allowed values: 1, 2 |
| `usyn_kw` | `string` | `varchar` |  |  | Used in Schedule (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rcmc_1` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rcmc_2` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rcmc_3` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rpmc_1` | `float` | `float` |  |  | Purchase Rate Multi Currency |
| `rpmc_2` | `float` | `float` |  |  | Purchase Rate Multi Currency |
| `rpmc_3` | `float` | `float` |  |  | Purchase Rate Multi Currency |
| `rsmc_1` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rsmc_2` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rsmc_3` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `svat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `psiu` | `integer` | `int` |  |  | Purchase Schedule in Use. Allowed values: 1, 2 |
| `psiu_kw` | `string` | `varchar` |  |  | Purchase Schedule in Use (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `ctyp` | `string` | `varchar` |  |  | Product Type. Max length: 3 |
| `cpcl` | `string` | `varchar` |  |  | Product Class. Max length: 6 |
| `cpln` | `string` | `varchar` |  |  | Product Line. Max length: 6 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `cood` | `string` | `varchar` |  |  | Technical Coordinator. Max length: 9 |
| `wght` | `float` | `float` |  |  | Weight |
| `cwun` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `dscb__en_us` | `string` | `varchar` |  | `not_null` | (required) Material - base language. Max length: 30 |
| `dscc__en_us` | `string` | `varchar` |  | `not_null` | (required) Size - base language. Max length: 30 |
| `dscd__en_us` | `string` | `varchar` |  | `not_null` | (required) Standard - base language. Max length: 30 |
| `ctyo` | `string` | `varchar` |  |  | Country of Origin. Max length: 3 |
| `cean` | `string` | `varchar` |  |  | EAN Code. Max length: 14 |
| `envc` | `integer` | `int` |  |  | Environmental Compliance. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40 |
| `envc_kw` | `string` | `varchar` |  |  | Environmental Compliance (keyword). Allowed values: tcenvc.not.appl, tcenvc.not.valid, tcenvc.compliant, tcenvc.compl.except, tcenvc.not.compl, tcenvc.cond.fail, tcenvc.part.fail, tcenvc.not.rele |
| `exin` | `string` | `varchar` |  |  | Extra Information. Max length: 8 |
| `ccde` | `string` | `varchar` |  |  | Harmonized System Code. Max length: 25 |
| `icsi` | `integer` | `int` |  |  | Critical Safety Item. Allowed values: 1, 2 |
| `icsi_kw` | `string` | `varchar` |  |  | Critical Safety Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rdpt` | `string` | `varchar` |  |  | Responsible Department. Max length: 6 |
| `pleq` | `string` | `varchar` |  |  | Planner. Max length: 9 |
| `mequ` | `integer` | `int` |  |  | Operated Equipment. Allowed values: 1, 2 |
| `mequ_kw` | `string` | `varchar` |  |  | Operated Equipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `seri` | `integer` | `int` |  |  | Serialized. Allowed values: 1, 2 |
| `seri_kw` | `string` | `varchar` |  |  | Serialized (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sprp` | `integer` | `int` |  |  | Self-Propelled. Allowed values: 1, 2 |
| `sprp_kw` | `string` | `varchar` |  |  | Self-Propelled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | General Text |
| `txtp` | `integer` | `int` |  |  | Purchase Text |
| `txts` | `integer` | `int` |  |  | Sales Text |
| `citg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs023 Item Groups |
| `csel_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs022 Selection Codes |
| `csig_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs018 Item Signals |
| `uset_cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs012 Units by Unit Set |
| `uset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs006 Unit Sets |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cccr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cpgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `csgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs044 Statistical Groups |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cpgs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `csgs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs044 Statistical Groups |
| `ccsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `srtr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs213 Services Trades |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `avtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `svat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `ctyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs015 Product Types |
| `cpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs062 Product Classes |
| `cpln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs061 Product Lines |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `cood_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ctyo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccde_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs028 Harmonized System Codes |
| `rdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `pleq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txts_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cequ_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Equipment |
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

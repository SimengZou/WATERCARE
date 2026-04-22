# qmpqm201

LN qmpqm201 Failure and Action Details table, Generated 2022-06-15T01:21:03Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmpqm201` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `frac`, `fpon` |
| **Column count** | 113 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `frac` | `string` | `varchar` | `PK` | `not_null` | (required) FRACAS Number. Max length: 9 |
| `fpon` | `integer` | `int` | `PK` | `not_null` | (required) Position |
| `item` | `string` | `varchar` |  |  | Reported Failed Item. Max length: 47 |
| `cser` | `string` | `varchar` |  |  | Reported Serial. Max length: 30 |
| `clot` | `string` | `varchar` |  |  | Reported Lot. Max length: 20 |
| `idat` | `timestamp` | `timestamp_ntz` |  |  | Inventory Date |
| `qnty` | `float` | `float` |  |  | Reported Quantity |
| `fqua` | `string` | `varchar` |  |  | Reported Unit. Max length: 3 |
| `aitm` | `string` | `varchar` |  |  | Actual Failed Item. Max length: 47 |
| `acsr` | `string` | `varchar` |  |  | Actual Serial. Max length: 30 |
| `alot` | `string` | `varchar` |  |  | Actual Lot. Max length: 20 |
| `nlot` | `string` | `varchar` |  |  | New Lot. Max length: 20 |
| `aqty` | `float` | `float` |  |  | Actual Quantity |
| `aqua` | `string` | `varchar` |  |  | Actual Unit. Max length: 3 |
| `frcd` | `string` | `varchar` |  |  | Failure Code. Max length: 9 |
| `rfdg` | `string` | `varchar` |  |  | Reference Designator. Max length: 16 |
| `rptd` | `integer` | `int` |  |  | Repeated Failure. Allowed values: 1, 2 |
| `rptd_kw` | `string` | `varchar` |  |  | Repeated Failure (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bdfx` | `integer` | `int` |  |  | Bad Fix. Allowed values: 1, 2 |
| `bdfx_kw` | `string` | `varchar` |  |  | Bad Fix (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `frct` | `string` | `varchar` |  |  | Failure Category. Max length: 9 |
| `frst` | `string` | `varchar` |  |  | Failure Stage. Max length: 9 |
| `intg` | `string` | `varchar` |  |  | Instrument Group. Max length: 6 |
| `inst` | `string` | `varchar` |  |  | Instrument. Max length: 6 |
| `inno` | `string` | `varchar` |  |  | Instrument Number. Max length: 9 |
| `frpt` | `string` | `varchar` |  |  | Technician. Max length: 9 |
| `quid` | `string` | `varchar` |  |  | Quarantine ID. Max length: 9 |
| `quil` | `integer` | `int` |  |  | Quarantine Disposition Line |
| `ncmr` | `string` | `varchar` |  |  | Non-Conformance Report. Max length: 9 |
| `capn` | `string` | `varchar` |  |  | Corrective Action Plan. Max length: 9 |
| `disa` | `integer` | `int` |  |  | Disposition Action. Allowed values: 0, 10, 20, 30, 40, 50, 60 |
| `disa_kw` | `string` | `varchar` |  |  | Disposition Action (keyword). Allowed values: , qmpqm.disa.awaiting, qmpqm.disa.rework, qmpqm.disa.replace, qmpqm.disa.non.conformance, qmpqm.disa.no.fault, qmpqm.disa.other |
| `disc` | `string` | `varchar` |  |  | Disposition Code. Max length: 9 |
| `disp` | `integer` | `int` |  |  | Disposition. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 20 |
| `disp_kw` | `string` | `varchar` |  |  | Disposition (keyword). Allowed values: qmncm.disp.rework.curr, qmncm.disp.rework.new, qmncm.disp.reclassify, qmncm.disp.return.vendor, qmncm.disp.scrap, qmncm.disp.use.as.is, qmncm.disp.repair, qmncm.disp.no.fault, qmncm.disp.undefined, qmncm.disp.not.appl |
| `fcrc` | `string` | `varchar` |  |  | Responsible. Max length: 9 |
| `disr` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `rsbp` | `string` | `varchar` |  |  | Responsible Business Partner. Max length: 9 |
| `titm` | `string` | `varchar` |  |  | To Item. Max length: 47 |
| `racd` | `string` | `varchar` |  |  | Action Code. Max length: 9 |
| `rpdt` | `timestamp` | `timestamp_ntz` |  |  | Repair Date |
| `ritm` | `string` | `varchar` |  |  | Replaced Item. Max length: 47 |
| `rcsr` | `string` | `varchar` |  |  | Replaced Serial. Max length: 30 |
| `rlot` | `string` | `varchar` |  |  | Replaced Lot. Max length: 20 |
| `rqty` | `float` | `float` |  |  | Replaced Quantity |
| `rqua` | `string` | `varchar` |  |  | Replaced Unit. Max length: 3 |
| `dorg` | `integer` | `int` |  |  | Disposition Order Origin. Allowed values: 0, 1, 2, 3, 10, 50, 51, 53, 55, 56, 61, 62, 63, 64, 65, 71, 72, 75, 76, 78, 80, 81, 82, 90, 95, 96, 97, 100 |
| `dorg_kw` | `string` | `varchar` |  |  | Disposition Order Origin (keyword). Allowed values: , qmpqm.orgn.sales, qmpqm.orgn.sales.sched, qmpqm.orgn.sales.man, qmpqm.orgn.adjustment, qmpqm.orgn.production, qmpqm.orgn.production.man, qmpqm.orgn.product.sched, qmpqm.orgn.product.asc, qmpqm.orgn.product.asc.man, qmpqm.orgn.service, qmpqm.orgn.maint.work, qmpqm.orgn.batch.repair, qmpqm.orgn.maint.sales, qmpqm.orgn.call, qmpqm.orgn.transfer, qmpqm.orgn.transfer.man, qmpqm.orgn.project, qmpqm.orgn.project.man, qmpqm.orgn.proj.contract, qmpqm.orgn.purchase, qmpqm.orgn.purchase.sched, qmpqm.orgn.purchase.man, qmpqm.orgn.enterprise.plan, qmpqm.orgn.stor.insp, qmpqm.orgn.warehouse.inv, qmpqm.orgn.inv.insp, qmpqm.orgn.not.appl |
| `dior` | `string` | `varchar` |  |  | Disposition Order. Max length: 9 |
| `diln` | `integer` | `int` |  |  | Disposition Order Position |
| `flsv` | `integer` | `int` |  |  | Failure Solved. Allowed values: 1, 2 |
| `flsv_kw` | `string` | `varchar` |  |  | Failure Solved (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `flcs` | `integer` | `int` |  |  | Failure Cannot be Solved. Allowed values: 1, 2 |
| `flcs_kw` | `string` | `varchar` |  |  | Failure Cannot be Solved (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ierd` | `integer` | `int` |  |  | Impact on Engineering Data Revision. Allowed values: 1, 2 |
| `ierd_kw` | `string` | `varchar` |  |  | Impact on Engineering Data Revision (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fsld` | `timestamp` | `timestamp_ntz` |  |  | Failure Solved Date |
| `flsr__en_us` | `string` | `varchar` |  | `not_null` | (required) Failure Solved Remarks - base language. Max length: 30 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30, 40, 50 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: qmpqm.flst.open, qmpqm.flst.assign, qmpqm.flst.disposition, qmpqm.flst.close, qmpqm.flst.cancel |
| `asby` | `string` | `varchar` |  |  | Last Assigned By. Max length: 16 |
| `asdt` | `timestamp` | `timestamp_ntz` |  |  | Assigned Date |
| `dpby` | `string` | `varchar` |  |  | Last Dispositioned By. Max length: 16 |
| `dpdt` | `timestamp` | `timestamp_ntz` |  |  | Dispositioned Date |
| `clby` | `string` | `varchar` |  |  | Closed By. Max length: 16 |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Closed Date |
| `caby` | `string` | `varchar` |  |  | Cancelled By. Max length: 16 |
| `cndt` | `timestamp` | `timestamp_ntz` |  |  | Cancelled Date |
| `psdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Date |
| `pcdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Completion Date |
| `asst` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Date |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Completion Date |
| `tipe` | `float` | `float` |  |  | Time Estimate |
| `tiun` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `oimg` | `string` | `varchar` |  |  | Old Picture. Max length: 22 |
| `nimg` | `string` | `varchar` |  |  | New Picture. Max length: 22 |
| `ftxt` | `integer` | `int` |  |  | Failure Text |
| `rtxt` | `integer` | `int` |  |  | Repair Text |
| `adtx` | `integer` | `int` |  |  | Additional Text |
| `frac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm200 Failure Report Analysis and Corrective Action System |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `fqua_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `aitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `aqua_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `frcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm022 Failure Codes |
| `rfdg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs090 Reference Designators |
| `frct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm021 Failure Categories |
| `frst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm020 Failure Stages |
| `intg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc024 Instrument Group |
| `ncmr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm100 Non-Conformance Report (NCR) |
| `capn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmcpl100 Corrective Action Plan |
| `disc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm007 Material Dispositions |
| `fcrc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm005 Responsibility |
| `disr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `rsbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `titm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `racd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm024 Action Codes |
| `ritm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `rqua_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `tiun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ftxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `rtxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `adtx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

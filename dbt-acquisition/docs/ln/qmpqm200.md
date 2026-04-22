# qmpqm200

LN qmpqm200 Failure Report Analysis and Corrective Action System table, Generated 2022-06-15T01:21:03Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmpqm200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `frac` |
| **Column count** | 63 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `frac` | `string` | `varchar` | `PK` | `not_null` | (required) FRACAS Number. Max length: 9 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `ftyp` | `integer` | `int` |  |  | FRACAS Type. Allowed values: 10, 20, 30, 40 |
| `ftyp_kw` | `string` | `varchar` |  |  | FRACAS Type (keyword). Allowed values: qmpqm.ftyp.hardware, qmpqm.ftyp.non.hardware, qmpqm.ftyp.instrument, qmpqm.ftyp.other |
| `othr__en_us` | `string` | `varchar` |  | `not_null` | (required) Specify Other - base language. Max length: 30 |
| `orgn` | `integer` | `int` |  |  | Order Origin. Allowed values: 1, 2, 3, 10, 50, 51, 53, 55, 56, 61, 62, 63, 64, 65, 71, 72, 75, 76, 78, 80, 81, 82, 90, 95, 96, 97, 100 |
| `orgn_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: qmpqm.orgn.sales, qmpqm.orgn.sales.sched, qmpqm.orgn.sales.man, qmpqm.orgn.adjustment, qmpqm.orgn.production, qmpqm.orgn.production.man, qmpqm.orgn.product.sched, qmpqm.orgn.product.asc, qmpqm.orgn.product.asc.man, qmpqm.orgn.service, qmpqm.orgn.maint.work, qmpqm.orgn.batch.repair, qmpqm.orgn.maint.sales, qmpqm.orgn.call, qmpqm.orgn.transfer, qmpqm.orgn.transfer.man, qmpqm.orgn.project, qmpqm.orgn.project.man, qmpqm.orgn.proj.contract, qmpqm.orgn.purchase, qmpqm.orgn.purchase.sched, qmpqm.orgn.purchase.man, qmpqm.orgn.enterprise.plan, qmpqm.orgn.stor.insp, qmpqm.orgn.warehouse.inv, qmpqm.orgn.inv.insp, qmpqm.orgn.not.appl |
| `orno` | `string` | `varchar` |  |  | Order Number. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Position |
| `seqn` | `integer` | `int` |  |  | Sequence |
| `opno` | `integer` | `int` |  |  | Operation |
| `styp` | `integer` | `int` |  |  | Line Type. Allowed values: 1, 2, 3, 4, 5 |
| `styp_kw` | `string` | `varchar` |  |  | Line Type (keyword). Allowed values: qmptc.styp.material.line, qmptc.styp.incoming.sub, qmptc.styp.outgoing.sub, qmptc.styp.activity.line, qmptc.styp.not.appl |
| `rpas` | `integer` | `int` |  |  | Reported at Supplier/Subcontractor. Allowed values: 0, 1, 2 |
| `rpas_kw` | `string` | `varchar` |  |  | Reported at Supplier/Subcontractor (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Reported Failed Item. Max length: 47 |
| `revi` | `string` | `varchar` |  |  | E-Item Revision. Max length: 6 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `cser` | `string` | `varchar` |  |  | Reported Serial. Max length: 30 |
| `clot` | `string` | `varchar` |  |  | Reported Lot. Max length: 20 |
| `pitm` | `string` | `varchar` |  |  | Production Item. Max length: 47 |
| `pser` | `string` | `varchar` |  |  | Production Serial Number. Max length: 30 |
| `plot` | `string` | `varchar` |  |  | Production Lot Number. Max length: 20 |
| `lfcr` | `string` | `varchar` |  |  | Parent FRACAS Number. Max length: 9 |
| `lfln` | `integer` | `int` |  |  | Parent FRACAS Line |
| `frst` | `string` | `varchar` |  |  | Failure Stage. Max length: 9 |
| `frcd` | `string` | `varchar` |  |  | Failure Code. Max length: 9 |
| `rptd` | `integer` | `int` |  |  | Repeated Failure. Allowed values: 1, 2 |
| `rptd_kw` | `string` | `varchar` |  |  | Repeated Failure (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `frct` | `string` | `varchar` |  |  | Failure Category. Max length: 9 |
| `intg` | `string` | `varchar` |  |  | Instrument Group. Max length: 6 |
| `inst` | `string` | `varchar` |  |  | Instrument. Max length: 6 |
| `inno` | `string` | `varchar` |  |  | Instrument Number. Max length: 9 |
| `frpt` | `string` | `varchar` |  |  | Failure Reported By. Max length: 16 |
| `fdat` | `timestamp` | `timestamp_ntz` |  |  | Failure Reported Date |
| `fcrt` | `string` | `varchar` |  |  | Failure Created By. Max length: 16 |
| `fsld` | `timestamp` | `timestamp_ntz` |  |  | Failure Solved Date |
| `cnrs` | `string` | `varchar` |  |  | Cancel Reason. Max length: 6 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 0, 10, 20, 30, 40, 50 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: , qmpqm.fsta.open, qmpqm.fsta.in.process, qmpqm.fsta.complete, qmpqm.fsta.close, qmpqm.fsta.cancel |
| `mrbc` | `string` | `varchar` |  |  | Team. Max length: 9 |
| `ftxt` | `integer` | `int` |  |  | Failure Text |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `pitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `frst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm020 Failure Stages |
| `frcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm022 Failure Codes |
| `frct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmpqm021 Failure Categories |
| `intg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc024 Instrument Group |
| `cnrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ftxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

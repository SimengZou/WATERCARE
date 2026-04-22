# qmcpl100

LN qmcpl100 Corrective Action Plan table, Generated 2022-06-15T01:21:00Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmcpl100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `capn` |
| **Column count** | 71 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `capn` | `string` | `varchar` | `PK` | `not_null` | (required) Corrective Action Plan. Max length: 9 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `appr` | `string` | `varchar` |  |  | Approver. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `revi` | `string` | `varchar` |  |  | E-Item Revision. Max length: 6 |
| `dudt` | `timestamp` | `timestamp_ntz` |  |  | Due Date |
| `cmdt` | `timestamp` | `timestamp_ntz` |  |  | Closed Date |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `cusr` | `string` | `varchar` |  |  | Created by User. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: qmcpl.caps.open, qmcpl.caps.submitted, qmcpl.caps.approved, qmcpl.caps.modified, qmcpl.caps.closed, qmcpl.caps.cancelled, qmcpl.caps.complete |
| `cprj` | `string` | `varchar` |  |  | Project Code. Max length: 9 |
| `catg` | `string` | `varchar` |  |  | Category. Max length: 9 |
| `apdt` | `timestamp` | `timestamp_ntz` |  |  | Approved Date |
| `sudt` | `timestamp` | `timestamp_ntz` |  |  | Submitted Date |
| `modt` | `timestamp` | `timestamp_ntz` |  |  | Modified Date |
| `cndt` | `timestamp` | `timestamp_ntz` |  |  | Cancelled Date |
| `cpef` | `integer` | `int` |  |  | CAP is Effective. Allowed values: 10, 20, 30, 100 |
| `cpef_kw` | `string` | `varchar` |  |  | CAP is Effective (keyword). Allowed values: qmcpl.cape.yes, qmcpl.cape.no, qmcpl.cape.uevl, qmcpl.cape.na |
| `ecpv` | `integer` | `int` |  |  | Effectiveness of CAP is Verified. Allowed values: 1, 2 |
| `ecpv_kw` | `string` | `varchar` |  |  | Effectiveness of CAP is Verified (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mrbc` | `string` | `varchar` |  |  | Review Board. Max length: 9 |
| `pbsm` | `integer` | `int` |  |  | Problem Solving Method. Allowed values: 10, 20, 30, 90, 100 |
| `pbsm_kw` | `string` | `varchar` |  |  | Problem Solving Method (keyword). Allowed values: tcmcs.pbsm.8d, tcmcs.pbsm.a3, tcmcs.pbsm.chlt, tcmcs.pbsm.other, tcmcs.pbsm.na |
| `psmo__en_us` | `string` | `varchar` |  | `not_null` | (required) Other Method - base language. Max length: 30 |
| `rcad` | `string` | `varchar` |  |  | RCA Document Number. Max length: 9 |
| `scap` | `integer` | `int` |  |  | Supplier Corrective Action Plan. Allowed values: 1, 2 |
| `scap_kw` | `string` | `varchar` |  |  | Supplier Corrective Action Plan (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scas` | `integer` | `int` |  |  | Supplier Corrective Action Plan Status. Allowed values: 10, 20, 30, 40, 50, 100 |
| `scas_kw` | `string` | `varchar` |  |  | Supplier Corrective Action Plan Status (keyword). Allowed values: qmcpl.scar.notsent, qmcpl.scar.submit, qmcpl.scar.receive, qmcpl.scar.approve, qmcpl.scar.reject, qmcpl.scar.na |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Due date for Effectiveness |
| `codt` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `text` | `integer` | `int` |  |  | Text |
| `pbst` | `integer` | `int` |  |  | Problem Statement |
| `comt` | `integer` | `int` |  |  | Comments |
| `bgif` | `integer` | `int` |  |  | Background Information |
| `crst` | `integer` | `int` |  |  | Current Situation |
| `goal` | `integer` | `int` |  |  | Goal |
| `cfef` | `integer` | `int` |  |  | Confirmation of Effect |
| `evds` | `integer` | `int` |  |  | Effectiveness Verification Details |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `appr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Number |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `catg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmcpl002 Categories |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `pbst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `comt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `bgif_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `crst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `goal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cfef_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `evds_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

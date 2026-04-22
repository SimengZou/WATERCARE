# qmptc100

LN qmptc100 Inspection Orders table, Generated 2022-06-15T01:21:05Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `iorn` |
| **Column count** | 101 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `iorn` | `string` | `varchar` | `PK` | `not_null` | (required) Inspection Order. Max length: 9 |
| `orgn` | `integer` | `int` |  |  | Origin. Allowed values: 1, 2, 4, 5, 6, 7, 8, 10, 11, 13, 20, 75, 80, 90, 100, 110, 120, 130, 140, 150, 200 |
| `orgn_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: qmptc.orgn.sales, qmptc.orgn.purchase, qmptc.orgn.production, qmptc.orgn.matl.bom, qmptc.orgn.rou.ti, qmptc.orgn.sales.sched, qmptc.orgn.purchase.sched, qmptc.orgn.cwar.insp, qmptc.orgn.inv.insp, qmptc.orgn.cwar.trans, qmptc.orgn.cwar.trans.man, qmptc.orgn.project, qmptc.orgn.prj.contract, qmptc.orgn.enterprise.plan, qmptc.orgn.prod.repetitive, qmptc.orgn.rou.repetitive, qmptc.orgn.service, qmptc.orgn.maint.sales, qmptc.orgn.batch.repair, qmptc.orgn.maint.work, qmptc.orgn.not.appl |
| `orno` | `string` | `varchar` |  |  | Origin Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Origin Order Line |
| `opno` | `integer` | `int` |  |  | Operation |
| `wstt` | `string` | `varchar` |  |  | Work Station. Max length: 6 |
| `seqn` | `integer` | `int` |  |  | Origin Order Sequence |
| `apno` | `string` | `varchar` |  |  | Inspection. Max length: 9 |
| `apsq` | `integer` | `int` |  |  | Inspection Sequence |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` |  |  | Location. Max length: 10 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `date` | `timestamp` | `timestamp_ntz` |  |  | Inventory Date |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `revi` | `string` | `varchar` |  |  | E-Item Revision. Max length: 6 |
| `quid` | `string` | `varchar` |  |  | Standard Test Procedure. Max length: 10 |
| `vrsn` | `integer` | `int` |  |  | Version |
| `tstg` | `integer` | `int` |  |  | Test Group |
| `tsty` | `integer` | `int` |  |  | Test Type. Allowed values: 1, 2, 3, 40, 50 |
| `tsty_kw` | `string` | `varchar` |  |  | Test Type (keyword). Allowed values: qmptc.tsty.hundred.perc, qmptc.tsty.single, qmptc.tsty.continuous, qmptc.tsty.sample.rule, qmptc.tsty.conf.rept |
| `freq` | `float` | `float` |  |  | Frequency |
| `cufr` | `string` | `varchar` |  |  | Frequency Unit. Max length: 3 |
| `psms` | `float` | `float` |  |  | Percentage |
| `sams` | `float` | `float` |  |  | Sample Size |
| `cuss` | `string` | `varchar` |  |  | Sample Unit. Max length: 3 |
| `aqls` | `integer` | `int` |  |  | AQL Switching. Allowed values: 1, 2 |
| `aqls_kw` | `string` | `varchar` |  |  | AQL Switching (keyword). Allowed values: qmptc.aqls.order, qmptc.aqls.characteristic |
| `aqll` | `float` | `float` |  |  | Order AQL |
| `qacc` | `integer` | `int` |  |  | Accept Quantity |
| `qrej` | `integer` | `int` |  |  | Reject Quantity |
| `craq` | `float` | `float` |  |  | Critical Characteristic AQL |
| `crac` | `integer` | `int` |  |  | Critical Characteristics Accepted Quantity |
| `crrj` | `integer` | `int` |  |  | Critical Characteristic Rejected Quantity |
| `mcaq` | `float` | `float` |  |  | Major Characteristic AQL |
| `mcac` | `integer` | `int` |  |  | Major Characteristics Accepted Quantity |
| `mcrj` | `integer` | `int` |  |  | Major Characteristic Rejected Quantity |
| `miaq` | `float` | `float` |  |  | Minor Characteristic AQL |
| `miac` | `integer` | `int` |  |  | Minor Characteristics Accepted Quantity |
| `mirj` | `integer` | `int` |  |  | Minor Characteristic Rejected Quantity |
| `saru` | `string` | `varchar` |  |  | Sampling Rule. Max length: 6 |
| `inss` | `string` | `varchar` |  |  | Inspection Standard. Max length: 6 |
| `inse` | `integer` | `int` |  |  | Inspection Severity. Allowed values: 10, 20, 30, 40, 100 |
| `inse_kw` | `string` | `varchar` |  |  | Inspection Severity (keyword). Allowed values: qmptc.inse.reduced, qmptc.inse.normal, qmptc.inse.tightened, qmptc.inse.first.article, qmptc.inse.not.appl |
| `ilvl` | `string` | `varchar` |  |  | Inspection Level. Max length: 6 |
| `sapl` | `string` | `varchar` |  |  | Sampling Plan. Max length: 6 |
| `noit` | `integer` | `int` |  |  | Number of Iterations |
| `iter` | `integer` | `int` |  |  | Iteration |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `sklo` | `integer` | `int` |  |  | Skip Lot. Allowed values: 1, 2 |
| `sklo_kw` | `string` | `varchar` |  |  | Skip Lot (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qtst` | `float` | `float` |  |  | Test Quantity |
| `tuni` | `string` | `varchar` |  |  | Test Unit. Max length: 3 |
| `cvss` | `float` | `float` |  |  | Conversion Factor Sample Size |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `reio` | `string` | `varchar` |  |  | Re-Inspection Order. Max length: 9 |
| `orio` | `string` | `varchar` |  |  | Original Inspection Order. Max length: 9 |
| `ostp` | `integer` | `int` |  |  | Order Specific Test Procedure. Allowed values: 1, 2 |
| `ostp_kw` | `string` | `varchar` |  |  | Order Specific Test Procedure (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pper` | `float` | `float` |  |  | Obsolete |
| `acre` | `integer` | `int` |  |  | Result. Allowed values: 10, 15, 20, 30 |
| `acre_kw` | `string` | `varchar` |  |  | Result (keyword). Allowed values: qmptc.acre.accept, qmptc.acre.part.accept, qmptc.acre.reject, qmptc.acre.undefined |
| `conr` | `integer` | `int` |  |  | Conformance Reporting. Allowed values: 1, 2 |
| `conr_kw` | `string` | `varchar` |  |  | Conformance Reporting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cncd` | `string` | `varchar` |  |  | Conformance Code. Max length: 6 |
| `team` | `string` | `varchar` |  |  | Team. Max length: 9 |
| `cwoc` | `string` | `varchar` |  |  | Work Center. Max length: 6 |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Inspection Order Text |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Number |
| `quid_vrsn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc010 Standard Test Procedures |
| `cufr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuss_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `saru_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc060 Sampling Rules |
| `inss_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc074 Inspection Standards |
| `ilvl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc064 Inspection Levels |
| `sapl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc070 Sampling Plans |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `tuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `reio_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc100 Inspection Orders |
| `orio_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc100 Inspection Orders |
| `cncd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmptc094 Conformance Reporting |
| `team_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl040 Teams |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
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

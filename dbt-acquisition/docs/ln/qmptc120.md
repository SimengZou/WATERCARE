# qmptc120

LN qmptc120 Order Inspections table, Generated 2022-06-15T01:21:06Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc120` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orgn`, `orno`, `pono`, `opno`, `wstt`, `seqn`, `apno`, `apsq`, `huid` |
| **Column count** | 107 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orgn` | `integer` | `int` | `PK` | `not_null` | (required) Origin. Allowed values: 1, 2, 4, 5, 6, 7, 8, 10, 11, 13, 20, 75, 80, 90, 100, 110, 120, 130, 140, 150, 200 |
| `orgn_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: qmptc.orgn.sales, qmptc.orgn.purchase, qmptc.orgn.production, qmptc.orgn.matl.bom, qmptc.orgn.rou.ti, qmptc.orgn.sales.sched, qmptc.orgn.purchase.sched, qmptc.orgn.cwar.insp, qmptc.orgn.inv.insp, qmptc.orgn.cwar.trans, qmptc.orgn.cwar.trans.man, qmptc.orgn.project, qmptc.orgn.prj.contract, qmptc.orgn.enterprise.plan, qmptc.orgn.prod.repetitive, qmptc.orgn.rou.repetitive, qmptc.orgn.service, qmptc.orgn.maint.sales, qmptc.orgn.batch.repair, qmptc.orgn.maint.work, qmptc.orgn.not.appl |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Origin Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Origin Order Line |
| `opno` | `integer` | `int` | `PK` | `not_null` | (required) Operation |
| `wstt` | `string` | `varchar` | `PK` | `not_null` | (required) Work Station. Max length: 6 |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Origin Order Sequence |
| `apno` | `string` | `varchar` | `PK` | `not_null` | (required) Inspection. Max length: 9 |
| `apsq` | `integer` | `int` | `PK` | `not_null` | (required) Inspection Sequence |
| `huid` | `string` | `varchar` | `PK` | `not_null` | (required) Handling Unit. Max length: 25 |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `date` | `timestamp` | `timestamp_ntz` |  |  | Inventory Date |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `revi` | `string` | `varchar` |  |  | E-Item Revision. Max length: 6 |
| `schd` | `integer` | `int` |  |  | Schedule. Allowed values: 0, 1, 2 |
| `schd_kw` | `string` | `varchar` |  |  | Schedule (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` |  |  | Location. Max length: 10 |
| `oqua` | `float` | `float` |  |  | Order Quantity |
| `cuoq` | `string` | `varchar` |  |  | Order Unit. Max length: 3 |
| `raqa` | `float` | `float` |  |  | Recommended Quantity Accepted |
| `rrqa` | `float` | `float` |  |  | Recommended Quantity Rejected |
| `rdqa` | `float` | `float` |  |  | Recommended Quantity Destroyed |
| `aaqa` | `float` | `float` |  |  | Actual Quantity Accepted |
| `arqa` | `float` | `float` |  |  | Actual Quantity Rejected |
| `cdis` | `string` | `varchar` |  |  | Reason for Rejection. Max length: 6 |
| `rsta` | `integer` | `int` |  |  | Inspection Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `rsta_kw` | `string` | `varchar` |  |  | Inspection Status (keyword). Allowed values: qmptc.rsta.free, qmptc.rsta.active, qmptc.rsta.completed, qmptc.rsta.processed, qmptc.rsta.closed, qmptc.rsta.cancelled |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `pdat` | `timestamp` | `timestamp_ntz` |  |  | Processing Date |
| `blmt` | `integer` | `int` |  |  | Blocking Method for Inspections. Allowed values: 1, 2, 3, 4, 14 |
| `blmt_kw` | `string` | `varchar` |  |  | Blocking Method for Inspections (keyword). Allowed values: qmptc.blmt.continue, qmptc.blmt.blck.oper, qmptc.blmt.blck.comp, qmptc.blmt.block, qmptc.blmt.not.appl |
| `reco` | `integer` | `int` |  |  | QM Only Recommends. Allowed values: 1, 2 |
| `reco_kw` | `string` | `varchar` |  |  | QM Only Recommends (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `gncm` | `integer` | `int` |  |  | Non-Conforming Material Report. Allowed values: 1, 2 |
| `gncm_kw` | `string` | `varchar` |  |  | Non-Conforming Material Report (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fain` | `string` | `varchar` |  |  | FAI Number. Max length: 9 |
| `fseq` | `integer` | `int` |  |  | FAI Sequence |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Activation Date |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Closing Date |
| `cndt` | `timestamp` | `timestamp_ntz` |  |  | Cancelled Date |
| `prmd` | `string` | `varchar` |  |  | Production Model. Max length: 9 |
| `pmrv` | `string` | `varchar` |  |  | Production Model Revision. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `stid` | `string` | `varchar` |  |  | Ship to Stock Rule ID. Max length: 22 |
| `stsn` | `string` | `varchar` |  |  | Ship to Stock Number. Max length: 9 |
| `stss` | `integer` | `int` |  |  | Ship to Stock. Allowed values: 1, 2 |
| `stss_kw` | `string` | `varchar` |  |  | Ship to Stock (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Number |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `stat_chno` | `integer` | `int` |  |  | CC: Status change number of Order Inspection |
| `oqua_buar` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Area |
| `oqua_buln` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Length |
| `oqua_bupc` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Piece |
| `oqua_butm` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Time |
| `oqua_buvl` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Volume |
| `oqua_buwg` | `float` | `float` |  |  | CC: Order Quantity in Base Unit Weight |
| `raqa_buar` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Area |
| `raqa_buln` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Length |
| `raqa_bupc` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Piece |
| `raqa_butm` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Time |
| `raqa_buvl` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Volume |
| `raqa_buwg` | `float` | `float` |  |  | CC: Recommended Quantity Accepted in Base Unit Weight |
| `rrqa_buar` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Area |
| `rrqa_buln` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Length |
| `rrqa_bupc` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Piece |
| `rrqa_butm` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Time |
| `rrqa_buvl` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Volume |
| `rrqa_buwg` | `float` | `float` |  |  | CC: Recommended Quantity Rejected in Base Unit Weight |
| `rdqa_buar` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Area |
| `rdqa_buln` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Length |
| `rdqa_bupc` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Piece |
| `rdqa_butm` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Time |
| `rdqa_buvl` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Volume |
| `rdqa_buwg` | `float` | `float` |  |  | CC: Recommended Quantity Destroyed in Base Unit Weight |
| `aaqa_buar` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Area |
| `aaqa_buln` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Length |
| `aaqa_bupc` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Piece |
| `aaqa_butm` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Time |
| `aaqa_buvl` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Volume |
| `aaqa_buwg` | `float` | `float` |  |  | CC: Actual Quantity Accepted in Base Unit Weight |
| `arqa_buar` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Area |
| `arqa_buln` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Length |
| `arqa_bupc` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Piece |
| `arqa_butm` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Time |
| `arqa_buvl` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Volume |
| `arqa_buwg` | `float` | `float` |  |  | CC: Actual Quantity Rejected in Base Unit Weight |
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

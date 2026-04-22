# qmptc112

LN qmptc112 Sample Statistical Measures table, Generated 2022-06-15T01:21:06Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmptc112` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `iorn`, `saml`, `ipon` |
| **Column count** | 46 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `iorn` | `string` | `varchar` | `PK` | `not_null` | (required) Inspection Order. Max length: 9 |
| `saml` | `integer` | `int` | `PK` | `not_null` | (required) Sample |
| `ipon` | `integer` | `int` | `PK` | `not_null` | (required) Inspection Order Line |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `aspt` | `string` | `varchar` |  |  | Aspect. Max length: 8 |
| `char` | `string` | `varchar` |  |  | Characteristic. Max length: 8 |
| `chun` | `string` | `varchar` |  |  | Characteristic Unit. Max length: 3 |
| `trun` | `string` | `varchar` |  |  | Transaction Unit. Max length: 3 |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `osta` | `integer` | `int` |  |  | Inspection Order Line Status. Allowed values: 1, 2, 3, 4, 5 |
| `osta_kw` | `string` | `varchar` |  |  | Inspection Order Line Status (keyword). Allowed values: qmptc.osta.free, qmptc.osta.printed, qmptc.osta.active, qmptc.osta.completed, qmptc.osta.cancelled |
| `acre` | `integer` | `int` |  |  | Inspection Order Line Result. Allowed values: 10, 15, 20, 30 |
| `acre_kw` | `string` | `varchar` |  |  | Inspection Order Line Result (keyword). Allowed values: qmptc.acre.accept, qmptc.acre.part.accept, qmptc.acre.reject, qmptc.acre.undefined |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Order Inspection Completion Date |
| `orgn` | `integer` | `int` |  |  | Order Origin. Allowed values: 1, 2, 4, 5, 6, 7, 8, 10, 11, 13, 20, 75, 80, 90, 100, 110, 120, 130, 140, 150, 200 |
| `orgn_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: qmptc.orgn.sales, qmptc.orgn.purchase, qmptc.orgn.production, qmptc.orgn.matl.bom, qmptc.orgn.rou.ti, qmptc.orgn.sales.sched, qmptc.orgn.purchase.sched, qmptc.orgn.cwar.insp, qmptc.orgn.inv.insp, qmptc.orgn.cwar.trans, qmptc.orgn.cwar.trans.man, qmptc.orgn.project, qmptc.orgn.prj.contract, qmptc.orgn.enterprise.plan, qmptc.orgn.prod.repetitive, qmptc.orgn.rou.repetitive, qmptc.orgn.service, qmptc.orgn.maint.sales, qmptc.orgn.batch.repair, qmptc.orgn.maint.work, qmptc.orgn.not.appl |
| `orno` | `string` | `varchar` |  |  | Origin Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Origin Order Line |
| `opno` | `integer` | `int` |  |  | Operation |
| `wstt` | `string` | `varchar` |  |  | Work Station. Max length: 6 |
| `seqn` | `integer` | `int` |  |  | Origin Order Sequence |
| `apno` | `string` | `varchar` |  |  | Warehouse Inspection. Max length: 9 |
| `apsq` | `integer` | `int` |  |  | Warehouse Inspection Sequence |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `mean` | `float` | `float` |  |  | Mean of Measurement Values |
| `mrav` | `float` | `float` |  |  | Moving Range Average |
| `rang` | `float` | `float` |  |  | Range |
| `stdv` | `float` | `float` |  |  | Standard Deviation |
| `nosp` | `integer` | `int` |  |  | No. of Sample Parts |
| `mivl` | `float` | `float` |  |  | Minimum Measurement Value |
| `mavl` | `float` | `float` |  |  | Maximum Measurement Value |
| `clwi` | `float` | `float` |  |  | Class Width |
| `text` | `integer` | `int` |  |  | Text |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

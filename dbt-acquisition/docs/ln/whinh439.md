# whinh439

LN whinh439 Deliveries table, Generated 2026-04-10T19:42:50Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh439` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `oorg`, `orno`, `pono`, `seqn`, `sqnb`, `shpm`, `shln` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `oorg` | `integer` | `int` | `PK` | `not_null` | (required) Order Origin. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `oorg_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Delivery Line |
| `shpm` | `string` | `varchar` | `PK` | `not_null` | (required) Shipment. Max length: 9 |
| `shln` | `integer` | `int` | `PK` | `not_null` | (required) Shipment Line |
| `fshp` | `integer` | `int` |  |  | Final Shipment. Allowed values: 1, 2 |
| `fshp_kw` | `string` | `varchar` |  |  | Final Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Confirmed Date |
| `ldat` | `timestamp` | `timestamp_ntz` |  |  | Log Date |
| `mess__en_us` | `string` | `varchar` |  | `not_null` | (required) Errors Logged - base language. Max length: 132 |
| `shpm_shln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh431 Shipment Lines |
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

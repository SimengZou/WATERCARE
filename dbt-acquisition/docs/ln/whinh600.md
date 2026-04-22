# whinh600

LN whinh600 Cross-dock Orders table, Generated 2026-04-10T19:42:52Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh600` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cdor` |
| **Column count** | 44 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cdor` | `string` | `varchar` | `PK` | `not_null` | (required) Cross-dock Order. Max length: 9 |
| `oorg` | `integer` | `int` |  |  | Order Origin. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `oorg_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `orno` | `string` | `varchar` |  |  | Outbound Order. Max length: 9 |
| `oset` | `integer` | `int` |  |  | Set |
| `pono` | `integer` | `int` |  |  | Line |
| `seqn` | `integer` | `int` |  |  | Sequence |
| `boml` | `integer` | `int` |  |  | BOM Line |
| `dlin` | `integer` | `int` |  |  | Project Peg Line |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `stlo` | `string` | `varchar` |  |  | Staging Location. Max length: 10 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `cdty` | `integer` | `int` |  |  | Cross-dock Order Type. Allowed values: 10, 20, 30 |
| `cdty_kw` | `string` | `varchar` |  |  | Cross-dock Order Type (keyword). Allowed values: whinh.cdty.static, whinh.cdty.dynamic, whinh.cdty.dms |
| `uspr` | `integer` | `int` |  |  | User Priority |
| `sypr` | `string` | `varchar` |  |  | System Priority. Max length: 80 |
| `cdpd` | `string` | `varchar` |  |  | Priority Definition. Max length: 3 |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `lcdt` | `timestamp` | `timestamp_ntz` |  |  | Last Change Date |
| `pddt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `qrun` | `float` | `float` |  |  | Requested Quantity to Cross-dock in Cross-dock Unit |
| `cdun` | `string` | `varchar` |  |  | Cross-dock Unit. Max length: 3 |
| `qreq` | `float` | `float` |  |  | Requested Quantity to Cross-dock |
| `qpla` | `float` | `float` |  |  | Planned Quantity to Cross-dock |
| `qapp` | `float` | `float` |  |  | Approved Quantity to Cross-dock |
| `qadv` | `float` | `float` |  |  | Advised Quantity to Cross-dock |
| `qact` | `float` | `float` |  |  | Cross-docked Quantity |
| `cdos` | `integer` | `int` |  |  | Cross-dock Order Status. Allowed values: 10, 20, 30, 40, 50, 60 |
| `cdos_kw` | `string` | `varchar` |  |  | Cross-dock Order Status (keyword). Allowed values: whinh.cdos.open, whinh.cdos.planned, whinh.cdos.in.process, whinh.cdos.staged, whinh.cdos.closed, whinh.cdos.canceled |
| `cwar_stlo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `cdpd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh620 Priority Definitions |
| `cdun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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

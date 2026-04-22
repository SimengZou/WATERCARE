# whina114

LN whina114 Inventory Receipt Transaction Consumptions table, Generated 2026-04-10T19:42:44Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina114` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `cwar`, `trdt`, `seqn`, `inwp`, `sern` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `trdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Date |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `inwp` | `integer` | `int` | `PK` | `not_null` | (required) Inventory WIP. Allowed values: 1, 2 |
| `inwp_kw` | `string` | `varchar` |  |  | Inventory WIP (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Consumption Part |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `ctdt` | `timestamp` | `timestamp_ntz` |  |  | Consumption Date |
| `qstk` | `float` | `float` |  |  | Consumed Quantity |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `tagn` | `string` | `varchar` |  |  | Tag. Max length: 9 |
| `lgdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Log Date |
| `ocmp` | `integer` | `int` |  |  | Order Company |
| `koor` | `integer` | `int` |  |  | Type of Order. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `koor_kw` | `string` | `varchar` |  |  | Type of Order (keyword). Allowed values: tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Line |
| `srnb` | `integer` | `int` |  |  | Sequence |
| `dlin` | `integer` | `int` |  |  | Project Peg Line |
| `itid` | `string` | `varchar` |  |  | Inventory Transaction ID. Max length: 9 |
| `itse` | `integer` | `int` |  |  | Inventory Transaction ID Sequence |
| `wvgr` | `string` | `varchar` |  |  | Warehouse Valuation Group. Max length: 6 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `reje` | `integer` | `int` |  |  | Quarantine Inventory. Allowed values: 1, 2 |
| `reje_kw` | `string` | `varchar` |  |  | Quarantine Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phtr` | `integer` | `int` |  |  | Physical Transaction. Allowed values: 1, 2 |
| `phtr_kw` | `string` | `varchar` |  |  | Physical Transaction (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `ocmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `wvgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina102 Warehouse Valuation Groups |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
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

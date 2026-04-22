# whina116

LN whina116 Inventory Variances table, Generated 2026-04-10T19:42:45Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina116` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `cwar`, `trdt`, `seqn` |
| **Column count** | 50 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `trdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Date |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `stkq` | `float` | `float` |  |  | Quantity |
| `koor` | `integer` | `int` |  |  | Type of Order. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `koor_kw` | `string` | `varchar` |  |  | Type of Order (keyword). Allowed values: tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Line |
| `srnb` | `integer` | `int` |  |  | Sequence |
| `dlin` | `integer` | `int` |  |  | Project Peg Line |
| `ivsq` | `integer` | `int` |  |  | Inventory Variance Sequence |
| `depc` | `integer` | `int` |  |  | Department Company |
| `wdep` | `string` | `varchar` |  |  | Department. Max length: 6 |
| `cbyi` | `integer` | `int` |  |  | To be Consumed by Inventory. Allowed values: 1, 2 |
| `cbyi_kw` | `string` | `varchar` |  |  | To be Consumed by Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `proc` | `integer` | `int` |  |  | Processed. Allowed values: 1, 2 |
| `proc_kw` | `string` | `varchar` |  |  | Processed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iror` | `integer` | `int` |  |  | Inventory Variance Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 100 |
| `iror_kw` | `string` | `varchar` |  |  | Inventory Variance Origin (keyword). Allowed values: tcvar.orig.price.change, tcvar.orig.invoice.var, tcvar.orig.prod.close, tcvar.orig.expensed.tax, tcvar.orig.efficiency.var, tcvar.orig.prod.price.var, tcvar.orig.lc.price.change, tcvar.orig.lc.invoice.var, tcvar.orig.lc.expensed.tax, tcvar.orig.curr.var.st.pm, tcvar.orig.exp.tax.st.pm, tcvar.orig.curr.var, tcvar.orig.curr.gain.loss, tcvar.orig.curr.var.lc, tcvar.orig.tax.corr, tcvar.orig.lc.tax.corr, tcvar.orig.exp.tax.reten, tcvar.orig.exp.tax.ret.rel, tcvar.orig.measure.result, tcvar.orig.prod.result, tcvar.orig.value.correc, tcvar.orig.not.appl |
| `cuso` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cuso_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Process Date |
| `rcno` | `string` | `varchar` |  |  | Receipt. Max length: 9 |
| `rcln` | `integer` | `int` |  |  | Receipt Line |
| `pyps` | `integer` | `int` |  |  | Payable Receipt Sequence |
| `lgdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Log Date |
| `wvgr` | `string` | `varchar` |  |  | Warehouse Valuation Group. Max length: 6 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `depc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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

# whinr110

LN whinr110 Inventory Transactions by Item and Warehouse table, Generated 2026-04-10T19:42:53Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinr110` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `cwar`, `trdt`, `seqn` |
| **Column count** | 113 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `trdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Date |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `tagn` | `string` | `varchar` |  |  | Tag. Max length: 9 |
| `qstk` | `float` | `float` |  |  | Quantity (Inventory Unit) |
| `qipu` | `float` | `float` |  |  | Quantity (Piece Unit) |
| `ocmp` | `integer` | `int` |  |  | Order Company |
| `koor` | `integer` | `int` |  |  | Order Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `koor_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `kost` | `integer` | `int` |  |  | Transaction Type. Allowed values: 1, 2, 3, 4, 5 |
| `kost_kw` | `string` | `varchar` |  |  | Transaction Type (keyword). Allowed values: tckost.stc.correction, tckost.on.order, tckost.receipt, tckost.allocation, tckost.issue |
| `itid` | `string` | `varchar` |  |  | Inventory Transaction ID. Max length: 9 |
| `itse` | `integer` | `int` |  |  | Inventory Transaction ID Sequence |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Line |
| `srnb` | `integer` | `int` |  |  | Sequence |
| `boml` | `integer` | `int` |  |  | BOM Line |
| `dlin` | `integer` | `int` |  |  | Project Peg Line |
| `rcno` | `string` | `varchar` |  |  | Receipt. Max length: 9 |
| `rcln` | `integer` | `int` |  |  | Receipt Line |
| `pyps` | `integer` | `int` |  |  | Payable Receipt Sequence |
| `shpm` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `shpo` | `integer` | `int` |  |  | Shipment Line |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `oats` | `string` | `varchar` |  |  | Order Attribute Set. Max length: 35 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `prjp` | `integer` | `int` |  |  | Project Pegged. Allowed values: 1, 2 |
| `prjp_kw` | `string` | `varchar` |  |  | Project Pegged (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prnt` | `integer` | `int` |  |  | Inventory Transaction Printed. Allowed values: 1, 2 |
| `prnt_kw` | `string` | `varchar` |  |  | Inventory Transaction Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `chan` | `string` | `varchar` |  |  | Channel. Max length: 3 |
| `qhnd` | `float` | `float` |  |  | Inventory After Transaction |
| `qaiu` | `float` | `float` |  |  | Attribute Set Inventory After Transaction (Inventory Unit) |
| `qapu` | `float` | `float` |  |  | Attribute Set Inventory After Transaction (Piece Unit) |
| `logn` | `string` | `varchar` |  |  | User. Max length: 16 |
| `cons` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3 |
| `cons_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: whconsignment.yes, whconsignment.no, whconsignment.shipment.var |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `phtr` | `integer` | `int` |  |  | Physical Transaction. Allowed values: 1, 2 |
| `phtr_kw` | `string` | `varchar` |  |  | Physical Transaction (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cosv` | `integer` | `int` |  |  | Consignment Shipment Variance. Allowed values: 1, 2 |
| `cosv_kw` | `string` | `varchar` |  |  | Consignment Shipment Variance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ctrr` | `integer` | `int` |  |  | Consignment Transfer Reversal. Allowed values: 0, 1, 2, 3, 255 |
| `ctrr_kw` | `string` | `varchar` |  |  | Consignment Transfer Reversal (keyword). Allowed values: , whinr.ctrr.phase.1, whinr.ctrr.phase.2, whinr.ctrr.phase.3, whinr.ctrr.not.applicable |
| `reco` | `integer` | `int` |  |  | Receipt Correction. Allowed values: 1, 2 |
| `reco_kw` | `string` | `varchar` |  |  | Receipt Correction (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `reje` | `integer` | `int` |  |  | Quarantine Inventory. Allowed values: 1, 2 |
| `reje_kw` | `string` | `varchar` |  |  | Quarantine Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vbas` | `integer` | `int` |  |  | Valuation by Attribute Set. Allowed values: 1, 2 |
| `vbas_kw` | `string` | `varchar` |  |  | Valuation by Attribute Set (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `valm` | `integer` | `int` |  |  | Valuation Method. Allowed values: 1, 2, 3, 4, 5, 6, 20, 255 |
| `valm_kw` | `string` | `varchar` |  |  | Valuation Method (keyword). Allowed values: whina.valm.ftp, whina.valm.mauc, whina.valm.fifo, whina.valm.lifo, whina.valm.lot, whina.valm.serial, whina.valm.tag, whina.valm.not.appl |
| `vwvg` | `integer` | `int` |  |  | Valuation by Warehouse Valuation Group. Allowed values: 1, 2 |
| `vwvg_kw` | `string` | `varchar` |  |  | Valuation by Warehouse Valuation Group (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wvgr` | `string` | `varchar` |  |  | Warehouse Valuation Group. Max length: 6 |
| `lgdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Log Date |
| `iseq` | `integer` | `int` |  |  | Invoice Sequence |
| `ttyp` | `string` | `varchar` |  |  | Financial Transaction Type. Max length: 3 |
| `cinv` | `integer` | `int` |  |  | Invoice Number |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `itrd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Transaction Date |
| `ilgd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Log Date |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `topl` | `integer` | `int` |  |  | Type of Order for Planning. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `topl_kw` | `string` | `varchar` |  |  | Type of Order for Planning (keyword). Allowed values: tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `ocmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `oats_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `chan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs066 Channels |
| `wvgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina102 Warehouse Valuation Groups |
| `qstk_buar` | `float` | `float` |  |  | CC: Quantity in Base Unit Area |
| `qstk_buln` | `float` | `float` |  |  | CC: Quantity in Base Unit Length |
| `qstk_buvl` | `float` | `float` |  |  | CC: Quantity in Base Unit Volume |
| `qstk_buwg` | `float` | `float` |  |  | CC: Quantity in Base Unit Weight |
| `qhnd_buar` | `float` | `float` |  |  | CC: Inventory After Transaction in Base Unit Area |
| `qhnd_buln` | `float` | `float` |  |  | CC: Inventory After Transaction in Base Unit Length |
| `qhnd_buvl` | `float` | `float` |  |  | CC: Inventory After Transaction in Base Unit Volume |
| `qhnd_buwg` | `float` | `float` |  |  | CC: Inventory After Transaction in Base Unit Weight |
| `koor_oorg` | `integer` | `int` |  |  | CC: Type of Order converted to Order Origin. Allowed values: 0, 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `koor_oorg_kw` | `string` | `varchar` |  |  | CC: Type of Order converted to Order Origin (keyword). Allowed values: , whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
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

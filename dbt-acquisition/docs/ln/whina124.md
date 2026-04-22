# whina124

LN whina124 Inventory Integration Transactions table, Generated 2026-04-10T19:42:46Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina124` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `guid` |
| **Column count** | 90 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `guid` | `string` | `varchar` | `PK` | `not_null` | (required) GUID. Max length: 22 |
| `itid` | `string` | `varchar` |  |  | Inventory Transaction ID. Max length: 9 |
| `itse` | `integer` | `int` |  |  | Inventory Transaction ID Sequence |
| `ivsq` | `integer` | `int` |  |  | Inventory Variance Sequence |
| `ocmp` | `integer` | `int` |  |  | Order Company |
| `rorg` | `integer` | `int` |  |  | Revaluation Origin. Allowed values: 10, 20, 30, 35, 40, 50, 100 |
| `rorg_kw` | `string` | `varchar` |  |  | Revaluation Origin (keyword). Allowed values: whina.rorg.act.cost.price, whina.rorg.antedating, whina.rorg.mauc.correct, whina.rorg.act.cost.corr, whina.rorg.change.val.meth, whina.rorg.change.int.fam, whina.rorg.not.appl |
| `rorn` | `string` | `varchar` |  |  | Revaluation Order. Max length: 9 |
| `rseq` | `integer` | `int` |  |  | Revaluation Order Sequence |
| `koor` | `integer` | `int` |  |  | Warehouse Type of Order. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `koor_kw` | `string` | `varchar` |  |  | Warehouse Type of Order (keyword). Allowed values: tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `orno` | `string` | `varchar` |  |  | Warehouse Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Order Line |
| `seqn` | `integer` | `int` |  |  | Order Sequence |
| `rcno` | `string` | `varchar` |  |  | Receipt. Max length: 9 |
| `rcln` | `integer` | `int` |  |  | Receipt Line |
| `dlin` | `integer` | `int` |  |  | Project Peg Line |
| `boml` | `integer` | `int` |  |  | BOM Line |
| `lcln` | `integer` | `int` |  |  | Landed Cost Line |
| `tror` | `integer` | `int` |  |  | Transaction Origin. Allowed values: 1, 2, 5, 7, 13, 14, 15, 16, 20, 21, 24, 25, 26, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 41, 50, 54, 55, 60, 61, 62, 65, 70, 71, 72, 81, 82, 90, 91, 95, 100, 102, 255 |
| `tror_kw` | `string` | `varchar` |  |  | Transaction Origin (keyword). Allowed values: tctror.pur, tctror.sls, tctror.pur.requisition, tctror.services.proc, tctror.pcs, tctror.prd, tctror.service, tctror.serv.call, tctror.prd.schedule, tctror.wrk.cl.cst.doc, tctror.proj.cost.comm, tctror.proj.revenues, tctror.proj.contract, tctror.payroll.advice, tctror.people, tctror.main.sales, tctror.main.work, tctror.pur.contracts, tctror.sls.contracts, tctror.pur.schedule, tctror.sls.schedule, tctror.assembly.line, tctror.assembly.order, tctror.serv.contract, tctror.transformation, tctror.adjustment, tctror.cycle.count, tctror.wh.issue, tctror.wh.receipt, tctror.wh.order, tctror.revaluation, tctror.freight.order, tctror.freight.shipm, tctror.freight.cluster, tctror.interest.inv, tctror.man.sls.inv, tctror.budget.procment, tctror.budget.wh.issue, tctror.cost.peg.trf, tctror.customer.claim, tctror.supplier.claim, tctror.not.applicable |
| `fitr` | `integer` | `int` |  |  | Financial Transaction. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 255 |
| `fitr_kw` | `string` | `varchar` |  |  | Financial Transaction (keyword). Allowed values: tcfitr.add.calc.offvar, tcfitr.allocation, tcfitr.retro.billing, tcfitr.appr.invoicing, tcfitr.add.cogs, tcfitr.add.general.res, tcfitr.contract, tcfitr.commission, tcfitr.completion, tcfitr.costs.spec, tcfitr.costs.spec.var, tcfitr.costs.interc, tcfitr.adjustment, tcfitr.add.completion, tcfitr.cogs.quotation, tcfitr.cogs.triangular, tcfitr.cogs.price.var, tcfitr.costs.comm, tcfitr.work.ord.pr.var, tcfitr.costs, tcfitr.costs.interc.wo, tcfitr.dir.del.iss, tcfitr.dir.del.iss.inv, tcfitr.dir.del.var, tcfitr.dir.del.inv, tcfitr.direct.delivery, tcfitr.line.surch, tcfitr.prod.result, tcfitr.direct.receipt, tcfitr.eff.var, tcfitr.issue.result, tcfitr.actual.costs, tcfitr.estimated.costs, tcfitr.expenses, tcfitr.interim.cogs, tcfitr.interim.revenue, tcfitr.int.res.invoic, tcfitr.var.adjustment, tcfitr.freight.pr.var, tcfitr.field.ch.order, tcfitr.freig.cost, tcfitr.freig.cost.inv, tcfitr.gen.costs.var, tcfitr.general.fin, tcfitr.general, tcfitr.general.hours, tcfitr.general.result, tcfitr.gen.costs.wo, tcfitr.gen.cogs.trian, tcfitr.interim.result, tcfitr.internal.servic, tcfitr.issue, tcfitr.wip.balance, tcfitr.issue.invoiced, tcfitr.rev.wip.balance, tcfitr.item.surch.rec, tcfitr.item.surch.iss, tcfitr.cons.reject, tcfitr.cons.issue, tcfitr.on.order, tcfitr.operation, tcfitr.order.disc, tcfitr.cons.receipt, tcfitr.cons.use, tcfitr.price.variance, tcfitr.trans.serv.wip, tcfitr.ftp.result, tcfitr.lot.result, tcfitr.project.surch, tcfitr.quarantine.rec, tcfitr.pur.price.var, tcfitr.rebate.res, tcfitr.rebate, tcfitr.receipt, tcfitr.rejection, tcfitr.deferred.rev, tcfitr.wip.revaluation, tcfitr.revaluation, tcfitr.revenue.finance, tcfitr.cons.reval, tcfitr.revenues, tcfitr.revenues.anal, tcfitr.cons.return, tcfitr.rev.interim.res, tcfitr.cons.issue.res, tcfitr.receip.invoiced, tcfitr.shipm.var.inv, tcfitr.shipm.variance, tcfitr.subcontracting, tcfitr.return.result, tcfitr.subcontr.wip, tcfitr.reject.invoiced, tcfitr.adv.inst.paid, tcfitr.advance.install, tcfitr.normal.install, tcfitr.settl.guar.inst, tcfitr.subcontr.inv, tcfitr.operation.inv, tcfitr.gen.cost.wo.inv, tcfitr.ship.var.inv.pr, tcfitr.gen.costs.inv, tcfitr.trian.invoiced, tcfitr.return.res.inv, tcfitr.trans.msc.inv, tcfitr.trans.accr, tcfitr.trans.msc, tcfitr.material, tcfitr.mat.invoiced, tcfitr.value.correct, tcfitr.repair.warranty, tcfitr.wip.hours, tcfitr.warranty.costs, tcfitr.wip.costs, tcfitr.wip.cst.finance, tcfitr.trans.proj.wip, tcfitr.wip.tran.issue, tcfitr.wip.tran.receip, tcfitr.wip.tran.issinv, tcfitr.wip.tran.recinv, tcfitr.warh.surch.iss, tcfitr.warh.surch.rec, tcfitr.holdback, tcfitr.wip.var.adjust, tcfitr.wip.value.corr, tcfitr.cogs, tcfitr.goodwill, tcfitr.tool.costs, tcfitr.tool.costs.inv, tcfitr.issue.inv.proj, tcfitr.ln.cost, tcfitr.ln.cost.var, tcfitr.ln.cost.inv, tcfitr.ln.cost.inv.var, tcfitr.invoiced.wip, tcfitr.issue.proj, tcfitr.appr.inv.proj, tcfitr.inv.value.corr, tcfitr.loan.result, tcfitr.so.price.var, tcfitr.cogs.int.inv, tcfitr.int.inv, tcfitr.plan.st.paym, tcfitr.rel.st.paym, tcfitr.receipt.st.paym, tcfitr.loan, tcfitr.borrow, tcfitr.payback.result, tcfitr.rec.var.st.paym, tcfitr.proj.rec.var.sp, tcfitr.cost.peg.trf.sp, tcfitr.curr.var.sp, tcfitr.approval, tcfitr.wip.trf.iss.ord, tcfitr.wip.trf.rec.ord, tcfitr.rec.var.cst.spc, tcfitr.exp.tax.st.paym, tcfitr.loan.reversal, tcfitr.borrow.reversal, tcfitr.curr.var, tcfitr.quarantine.iss, tcfitr.wip.quar.iss, tcfitr.wip.quar.rec, tcfitr.quarantine.adj, tcfitr.wip.quar.adj, tcfitr.quarantine.rev, tcfitr.wip.quar.rev, tcfitr.curr.var.lc, tcfitr.progr.payment, tcfitr.rev.interc, tcfitr.work.cell.sur, tcfitr.wip.repair.iss, tcfitr.wip.costs.ic, tcfitr.wip.hours.ic, tcfitr.gen.hours.ic, tcfitr.expenses.ic, tcfitr.costs.ic.hours, tcfitr.costs.ic.exp, tcfitr.issue.res.proj, tcfitr.ln.cost.ic, tcfitr.ln.cost.inv.ic, tcfitr.rec.ic.st.paym, tcfitr.dir.rec.ic, tcfitr.variance.ic, tcfitr.costs.ic.pur, tcfitr.cogs.variance, tcfitr.cogs.quot.var, tcfitr.cogs.reval, tcfitr.cons.var.adj, tcfitr.cons.val.corr, tcfitr.cos.inv, tcfitr.cos.rec, tcfitr.cos.quot.inv, tcfitr.cos.quot.rec, tcfitr.rev.inv, tcfitr.rev.rec, tcfitr.ord.disc.inv, tcfitr.ord.disc.rec, tcfitr.cos.inv.proj, tcfitr.rev.inv.proj, tcfitr.ord.disc.inv.pr, tcfitr.afp.commit, tcfitr.rental, tcfitr.rental.ic, tcfitr.costs.ic.rental, tcfitr.tr.serv.wip.ic, tcfitr.tr.proj.wip.ic, tcfitr.costs.ic.serv, tcfitr.rent.sh.var.ic, tcfitr.ftp.result.proj, tcfitr.measure.result, tcfitr.rental.cos, tcfitr.rent.rev.anal, tcfitr.rent.rev, tcfitr.not.applicable |
| `cuso` | `integer` | `int` |  |  | Customer Owned. Allowed values: 1, 2 |
| `cuso_kw` | `string` | `varchar` |  |  | Customer Owned (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `expt` | `integer` | `int` |  |  | Expense Tax. Allowed values: 1, 2 |
| `expt_kw` | `string` | `varchar` |  |  | Expense Tax (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Date |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `itmf` | `string` | `varchar` |  |  | Item From. Max length: 47 |
| `itmt` | `string` | `varchar` |  |  | Item To. Max length: 47 |
| `atsf` | `string` | `varchar` |  |  | Attribute Set From. Max length: 35 |
| `atst` | `string` | `varchar` |  |  | Attribute Set To. Max length: 35 |
| `quaf` | `float` | `float` |  |  | Quantity From |
| `quat` | `float` | `float` |  |  | Quantity To |
| `unif` | `string` | `varchar` |  |  | Unit From. Max length: 3 |
| `unit` | `string` | `varchar` |  |  | Unit To. Max length: 3 |
| `logf` | `integer` | `int` |  |  | Logistic Company From |
| `typf` | `integer` | `int` |  |  | Entity Type From. Allowed values: 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 15, 20 |
| `typf_kw` | `string` | `varchar` |  |  | Entity Type From (keyword). Allowed values: tcemm.enty.sls.office, tcemm.enty.pur.office, tcemm.enty.service.dept, tcemm.enty.workcenter, tcemm.enty.warehouse, tcemm.enty.project, tcemm.enty.accounting, tcemm.enty.contract, tcemm.enty.shipping.office, tcemm.enty.proj.mgt.office, tcemm.enty.production.dept, tcemm.enty.inv.man.dept, tcemm.enty.not.appl |
| `codf` | `string` | `varchar` |  |  | Entity Code From. Max length: 9 |
| `logt` | `integer` | `int` |  |  | Logistic Company To |
| `typt` | `integer` | `int` |  |  | Entity Type To. Allowed values: 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 15, 20 |
| `typt_kw` | `string` | `varchar` |  |  | Entity Type To (keyword). Allowed values: tcemm.enty.sls.office, tcemm.enty.pur.office, tcemm.enty.service.dept, tcemm.enty.workcenter, tcemm.enty.warehouse, tcemm.enty.project, tcemm.enty.accounting, tcemm.enty.contract, tcemm.enty.shipping.office, tcemm.enty.proj.mgt.office, tcemm.enty.production.dept, tcemm.enty.inv.man.dept, tcemm.enty.not.appl |
| `codt` | `string` | `varchar` |  |  | Entity Code To. Max length: 9 |
| `ccpf` | `string` | `varchar` |  |  | Cost Component From. Max length: 8 |
| `ccpt` | `string` | `varchar` |  |  | Cost Component To. Max length: 8 |
| `houf` | `float` | `float` |  |  | Hours From |
| `hout` | `float` | `float` |  |  | Hours To |
| `amtf_1` | `float` | `float` |  |  | Amount From |
| `amtf_2` | `float` | `float` |  |  | Amount From |
| `amtf_3` | `float` | `float` |  |  | Amount From |
| `amtt_1` | `float` | `float` |  |  | Amount To |
| `amtt_2` | `float` | `float` |  |  | Amount To |
| `amtt_3` | `float` | `float` |  |  | Amount To |
| `ocmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `itmf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `itmt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `atsf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `atst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `unif_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `unit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccpf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `ccpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `ccpf_cpcp` | `string` | `varchar` |  |  | CC: Cost Component. Max length: 8 |
| `codf_cwar` | `string` | `varchar` |  |  | CC: Warehouse. Max length: 6 |
| `houf_hour` | `float` | `float` |  |  | CC: Hours |
| `itmf_item` | `string` | `varchar` |  |  | CC: Item. Max length: 47 |
| `amnt_lclc` | `float` | `float` |  |  | CC: Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Amount in Reporting Currency 2 |
| `amnt_rfrc` | `float` | `float` |  |  | CC: Amount in Reference Currency |
| `amnt_dtwc` | `float` | `float` |  |  | CC: Amount in Data Warehouse Currency |
| `quan_invu` | `float` | `float` |  |  | CC: Quantity in Inventory Unit |
| `quan_buar` | `float` | `float` |  |  | CC: Quantity in Base Unit Area |
| `quan_buln` | `float` | `float` |  |  | CC: Quantity in Base Unit Length |
| `quan_buvl` | `float` | `float` |  |  | CC: Quantity in Base Unit Volume |
| `quan_buwg` | `float` | `float` |  |  | CC: Quantity in Base Unit Weight |
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

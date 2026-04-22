# tpppc200

LN tpppc200 Cost Transactions table, Generated 2026-04-10T19:42:07Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cspa`, `cotp`, `coob`, `sern` |
| **Column count** | 265 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `cotp` | `integer` | `int` | `PK` | `not_null` | (required) Cost Type. Allowed values: 1, 2, 3, 4, 5, 10 |
| `cotp_kw` | `string` | `varchar` |  |  | Cost Type (keyword). Allowed values: tppdm.cotp.tasks, tppdm.cotp.materials, tppdm.cotp.equipment, tppdm.cotp.subcontracting, tppdm.cotp.indirect, tppdm.cotp.overhead |
| `coob` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Object. Max length: 47 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `phit` | `string` | `varchar` |  |  | Phantom Item. Max length: 47 |
| `efph` | `integer` | `int` |  |  | Effectivity Unit Phantom |
| `ptyc` | `integer` | `int` |  |  | Posting Type. Allowed values: 1, 7, 13, 17, 19, 22, 23, 24, 26, 31, 34, 37, 38, 39, 40, 41, 43, 44, 45, 48, 50, 52, 54, 56, 57, 60, 61, 65, 66, 67, 68, 70, 71, 74, 75, 78, 81, 82, 84, 86, 87, 88, 89, 91, 92, 97, 98, 99, 100, 101, 107, 108, 109, 110, 111, 112, 114, 116, 117, 119, 120, 121, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 138, 139, 140, 142, 143, 144, 145, 146, 147, 148, 150, 151, 152, 156, 157, 158, 159, 161, 163, 167, 169, 172, 173, 175, 179, 180, 181, 182, 183, 185, 191, 198, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225 |
| `ptyc_kw` | `string` | `varchar` |  |  | Posting Type (keyword). Allowed values: tpppc.ptyc.add.calc.offvar, tpppc.ptyc.contract, tpppc.ptyc.adjustment, tpppc.ptyc.cogs.price.var, tpppc.ptyc.work.ord.pr.var, tpppc.ptyc.dir.del.iss, tpppc.ptyc.dir.del.iss.inv, tpppc.ptyc.dir.del.var, tpppc.ptyc.direct.delivery, tpppc.ptyc.issue.result, tpppc.ptyc.expenses, tpppc.ptyc.int.res.invoic, tpppc.ptyc.var.adjustment, tpppc.ptyc.freight.pr.var, tpppc.ptyc.field.ch.order, tpppc.ptyc.freig.cost, tpppc.ptyc.gen.costs.var, tpppc.ptyc.general.fin, tpppc.ptyc.general, tpppc.ptyc.gen.costs.wo, tpppc.ptyc.interim.result, tpppc.ptyc.issue, tpppc.ptyc.issue.invoiced, tpppc.ptyc.item.surch.rec, tpppc.ptyc.item.surch.iss, tpppc.ptyc.on.order, tpppc.ptyc.operation, tpppc.ptyc.price.variance, tpppc.ptyc.trans.serv.wip, tpppc.ptyc.tr.serv.wip.ic, tpppc.ptyc.ftp.result, tpppc.ptyc.quarantine.rec, tpppc.ptyc.pur.price.var, tpppc.ptyc.receipt, tpppc.ptyc.rejection, tpppc.ptyc.revaluation, tpppc.ptyc.revenues, tpppc.ptyc.revenues.anal, tpppc.ptyc.rev.interim.res, tpppc.ptyc.receip.invoiced, tpppc.ptyc.shipm.var.inv, tpppc.ptyc.shipment.var, tpppc.ptyc.subcontracting, tpppc.ptyc.subcontr.wip, tpppc.ptyc.reject.invoiced, tpppc.ptyc.subcontr.inv, tpppc.ptyc.operation.inv, tpppc.ptyc.gen.cost.wo.inv, tpppc.ptyc.ship.var.inv.pr, tpppc.ptyc.gen.costs.inv, tpppc.ptyc.material, tpppc.ptyc.mat.invoiced, tpppc.ptyc.value.correct, tpppc.ptyc.repair.warranty, tpppc.ptyc.wip.hours, tpppc.ptyc.warranty.costs, tpppc.ptyc.wip.cst.finance, tpppc.ptyc.wip.tran.issue, tpppc.ptyc.wip.tran.receip, tpppc.ptyc.wip.tran.recinv, tpppc.ptyc.warh.surch.iss, tpppc.ptyc.warh.surch.rec, tpppc.ptyc.cogs, tpppc.ptyc.goodwill, tpppc.ptyc.tool.costs, tpppc.ptyc.tool.costs.inv, tpppc.ptyc.issue.inv.proj, tpppc.ptyc.ln.cost, tpppc.ptyc.ln.cost.var, tpppc.ptyc.ln.cost.inv, tpppc.ptyc.ln.cost.inv.var, tpppc.ptyc.invoiced.wip, tpppc.ptyc.issue.proj, tpppc.ptyc.loan.result, tpppc.ptyc.so.price.var, tpppc.ptyc.cogs.int.inv, tpppc.ptyc.plan.st.paym, tpppc.ptyc.rel.st.paym, tpppc.ptyc.receipt.st.paym, tpppc.ptyc.loan, tpppc.ptyc.borrow, tpppc.ptyc.payback.result, tpppc.ptyc.rec.var.st.paym, tpppc.ptyc.cost.peg.trf.sp, tpppc.ptyc.curr.var.sp, tpppc.ptyc.approval, tpppc.ptyc.exp.tax.st.paym, tpppc.ptyc.loan.reversal, tpppc.ptyc.borrow.reversal, tpppc.ptyc.curr.var, tpppc.ptyc.wip.quar.iss, tpppc.ptyc.quarantine.adj, tpppc.ptyc.curr.var.lc, tpppc.ptyc.rev.interc, tpppc.ptyc.wip.costs.ic, tpppc.ptyc.wip.hours.ic, tpppc.ptyc.expenses.ic, tpppc.ptyc.ln.cost.ic, tpppc.ptyc.ln.cost.inv.ic, tpppc.ptyc.rec.ic.st.paym, tpppc.ptyc.dir.rec.ic, tpppc.ptyc.variance.ic, tpppc.ptyc.cogs.variance, tpppc.ptyc.cos.rec, tpppc.ptyc.cos.inv.proj, tpppc.ptyc.cmt.soft.pc, tpppc.ptyc.cmt.soft.pc.rev, tpppc.ptyc.receipt.prj.wrh, tpppc.ptyc.matched.receipt, tpppc.ptyc.project.control, tpppc.ptyc.inventory, tpppc.ptyc.cost.invoice, tpppc.ptyc.price.var.rev, tpppc.ptyc.expense.tax, tpppc.ptyc.ln.cost.exp.tax, tpppc.ptyc.pur.invoice, tpppc.ptyc.ln.cost.wo.invc, tpppc.ptyc.field.service, tpppc.ptyc.labor, tpppc.ptyc.hours.subcontr, tpppc.ptyc.manual.sales, tpppc.ptyc.charge.soft.cmt, tpppc.ptyc.charge.hard.cmt, tpppc.ptyc.charge.cost, tpppc.ptyc.applied.ovhd, tpppc.ptyc.stage.invoice, tpppc.ptyc.cost.peg.trf.ex, tpppc.ptyc.curr.gain.loss, tpppc.ptyc.afp.commit, tpppc.ptyc.rental, tpppc.ptyc.rental.ic |
| `quan` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `time` | `float` | `float` |  |  | Amount of Time |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `curc` | `string` | `varchar` |  |  | Cost Currency. Max length: 3 |
| `cohc_1` | `float` | `float` |  |  | Cost Amount in Home Currency (Credit) |
| `cohc_2` | `float` | `float` |  |  | Cost Amount in Home Currency (Credit) |
| `cohc_3` | `float` | `float` |  |  | Cost Amount in Home Currency (Credit) |
| `cohd_1` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `cohd_2` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `cohd_3` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `rtcc_1` | `float` | `float` |  |  | Currency Rate (Costs - Debit) |
| `rtcc_2` | `float` | `float` |  |  | Currency Rate (Costs - Debit) |
| `rtcc_3` | `float` | `float` |  |  | Currency Rate (Costs - Debit) |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor (Costs - Debit) |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor (Costs - Debit) |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor (Costs - Debit) |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type (Costs). Max length: 3 |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date (Costs) |
| `fcrt` | `integer` | `int` |  |  | Rate Determiner (Costs). Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Rate Determiner (Costs) (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cnln` | `string` | `varchar` |  |  | Contract Line. Max length: 8 |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Progress Date |
| `lcor` | `integer` | `int` |  |  | Logistic Company of Document |
| `koor` | `integer` | `int` |  |  | Order Type. Allowed values: 1, 2, 3, 6, 7, 9, 10, 16, 17, 18, 19, 22, 26, 28, 31, 33, 36, 37, 38, 43, 51, 52, 55, 56, 57, 58, 59, 60, 70, 78, 80, 82, 84, 86, 88, 90, 92, 93, 95, 96, 97, 100, 200, 201, 202 |
| `koor_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tpppc.koor.act.sfc, tpppc.koor.act.pur, tpppc.koor.act.sls, tpppc.koor.act.pur.sched, tpppc.koor.act.sls.sched, tpppc.koor.act.asc, tpppc.koor.services.proc, tpppc.koor.wrh.order, tpppc.koor.act.srv, tpppc.koor.pss.pur, tpppc.koor.pss.wrh, tpppc.koor.act.trf, tpppc.koor.act.pmg, tpppc.koor.srv.call, tpppc.koor.cf.ap, tpppc.koor.act.pur.man, tpppc.koor.act.trf.man, tpppc.koor.act.srv.sls, tpppc.koor.act.dpt.wrk, tpppc.koor.freight, tpppc.koor.cycle.count, tpppc.koor.adjustment, tpppc.koor.revaluation, tpppc.koor.product.sched, tpppc.koor.product.kanban, tpppc.koor.project, tpppc.koor.project.man, tpppc.koor.enterprise.plan, tpppc.koor.inv.own.change, tpppc.koor.bfbp.trf.pur, tpppc.koor.bfbp.trf.sched, tpppc.koor.stbp.trf.sls, tpppc.koor.stbp.trf.sched, tpppc.koor.stbp.trf.wh.man, tpppc.koor.stbp.trf.man, tpppc.koor.stbp.trf.wh.dis, tpppc.koor.cp.cpt, tpppc.koor.act.cpt, tpppc.koor.proj.contract, tpppc.koor.customer.claim, tpppc.koor.supplier.claim, tpppc.koor.not.appl, tpppc.koor.srv.call.obs, tpppc.koor.srv.contract, tpppc.koor.financial.doc |
| `orno` | `string` | `varchar` |  |  | Order Number. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Order Line |
| `srnb` | `integer` | `int` |  |  | Order Line Sequence |
| `rsqn` | `integer` | `int` |  |  | Purchase Receipt Sequence |
| `vrln` | `integer` | `int` |  |  | Variation Line |
| `clin` | `integer` | `int` |  |  | Services Procurement Order Call-Off Line |
| `lcln` | `integer` | `int` |  |  | Landed Cost Line Number |
| `spln` | `integer` | `int` |  |  | Stage Payment Line |
| `rcno` | `string` | `varchar` |  |  | Warehouse Receipt / Shipment. Max length: 9 |
| `rcln` | `integer` | `int` |  |  | Warehouse Receipt / Shipment Line |
| `afpy` | `string` | `varchar` |  |  | Application for Payment. Max length: 9 |
| `rttp` | `integer` | `int` |  |  | Retention Type. Allowed values: 10, 20, 255 |
| `rttp_kw` | `string` | `varchar` |  |  | Retention Type (keyword). Allowed values: tcrttp.deducted, tcrttp.released, tcrttp.not.appl |
| `rlin` | `integer` | `int` |  |  | Retention Line |
| `bona` | `string` | `varchar` |  |  | Business Object Name. Max length: 17 |
| `borf` | `string` | `varchar` |  |  | Business Object Reference. Max length: 90 |
| `obon` | `string` | `varchar` |  |  | Originating Business Object Name. Max length: 17 |
| `oboi` | `string` | `varchar` |  |  | Originating Business Object ID. Max length: 11 |
| `obor` | `string` | `varchar` |  |  | Originating Business Object Reference. Max length: 90 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `srni` | `integer` | `int` |  |  | Invoice Batch Number |
| `cdoc__en_us` | `string` | `varchar` |  | `not_null` | (required) Document - base language. Max length: 10 |
| `ncmp` | `integer` | `int` |  |  | Company Financial Document |
| `ftty` | `string` | `varchar` |  |  | Transaction Type Financial Transaction. Max length: 3 |
| `fdoc` | `integer` | `int` |  |  | Document Financial Transaction |
| `flin` | `integer` | `int` |  |  | Line Number Financial Transaction |
| `fsrl` | `integer` | `int` |  |  | Sequence Number Finance |
| `ftln` | `integer` | `int` |  |  | Target Line Number |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `cptc` | `string` | `varchar` |  |  | Period Table Cost Control. Max length: 6 |
| `year` | `integer` | `int` |  |  | Cost Control Year |
| `peri` | `integer` | `int` |  |  | Cost Control Period |
| `frgd` | `timestamp` | `timestamp_ntz` |  |  | Financial Registration Date |
| `cptf` | `string` | `varchar` |  |  | Period Table Fiscal Period. Max length: 6 |
| `fyea` | `integer` | `int` |  |  | Fiscal Year |
| `fper` | `integer` | `int` |  |  | Fiscal Period |
| `frdc` | `timestamp` | `timestamp_ntz` |  |  | Financial Registration Date at Completion |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `drun` | `timestamp` | `timestamp_ntz` |  |  | Run Date |
| `serb` | `integer` | `int` |  |  | Processing Run Number |
| `cdru` | `timestamp` | `timestamp_ntz` |  |  | Run Date (Report Completed) |
| `cser` | `integer` | `int` |  |  | Run Sequence No. (Report Completed) |
| `ocmp` | `integer` | `int` |  |  | Receiving Company |
| `enty` | `integer` | `int` |  |  | Entity Type. Allowed values: 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 15, 20 |
| `enty_kw` | `string` | `varchar` |  |  | Entity Type (keyword). Allowed values: tcemm.enty.sls.office, tcemm.enty.pur.office, tcemm.enty.service.dept, tcemm.enty.workcenter, tcemm.enty.warehouse, tcemm.enty.project, tcemm.enty.accounting, tcemm.enty.contract, tcemm.enty.shipping.office, tcemm.enty.proj.mgt.office, tcemm.enty.production.dept, tcemm.enty.inv.man.dept, tcemm.enty.not.appl |
| `enid` | `string` | `varchar` |  |  | Entity. Max length: 9 |
| `entu` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `fcor` | `integer` | `int` |  |  | Financial Company of Origin |
| `tror` | `integer` | `int` |  |  | Transaction Origin. Allowed values: 1, 2, 5, 7, 13, 14, 15, 16, 20, 21, 24, 25, 26, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 41, 50, 54, 55, 60, 61, 62, 65, 70, 71, 72, 81, 82, 90, 91, 95, 100, 102, 255 |
| `tror_kw` | `string` | `varchar` |  |  | Transaction Origin (keyword). Allowed values: tctror.pur, tctror.sls, tctror.pur.requisition, tctror.services.proc, tctror.pcs, tctror.prd, tctror.service, tctror.serv.call, tctror.prd.schedule, tctror.wrk.cl.cst.doc, tctror.proj.cost.comm, tctror.proj.revenues, tctror.proj.contract, tctror.payroll.advice, tctror.people, tctror.main.sales, tctror.main.work, tctror.pur.contracts, tctror.sls.contracts, tctror.pur.schedule, tctror.sls.schedule, tctror.assembly.line, tctror.assembly.order, tctror.serv.contract, tctror.transformation, tctror.adjustment, tctror.cycle.count, tctror.wh.issue, tctror.wh.receipt, tctror.wh.order, tctror.revaluation, tctror.freight.order, tctror.freight.shipm, tctror.freight.cluster, tctror.interest.inv, tctror.man.sls.inv, tctror.budget.procment, tctror.budget.wh.issue, tctror.cost.peg.trf, tctror.customer.claim, tctror.supplier.claim, tctror.not.applicable |
| `fitr` | `integer` | `int` |  |  | Financial Transaction. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 255 |
| `fitr_kw` | `string` | `varchar` |  |  | Financial Transaction (keyword). Allowed values: tcfitr.add.calc.offvar, tcfitr.allocation, tcfitr.retro.billing, tcfitr.appr.invoicing, tcfitr.add.cogs, tcfitr.add.general.res, tcfitr.contract, tcfitr.commission, tcfitr.completion, tcfitr.costs.spec, tcfitr.costs.spec.var, tcfitr.costs.interc, tcfitr.adjustment, tcfitr.add.completion, tcfitr.cogs.quotation, tcfitr.cogs.triangular, tcfitr.cogs.price.var, tcfitr.costs.comm, tcfitr.work.ord.pr.var, tcfitr.costs, tcfitr.costs.interc.wo, tcfitr.dir.del.iss, tcfitr.dir.del.iss.inv, tcfitr.dir.del.var, tcfitr.dir.del.inv, tcfitr.direct.delivery, tcfitr.line.surch, tcfitr.prod.result, tcfitr.direct.receipt, tcfitr.eff.var, tcfitr.issue.result, tcfitr.actual.costs, tcfitr.estimated.costs, tcfitr.expenses, tcfitr.interim.cogs, tcfitr.interim.revenue, tcfitr.int.res.invoic, tcfitr.var.adjustment, tcfitr.freight.pr.var, tcfitr.field.ch.order, tcfitr.freig.cost, tcfitr.freig.cost.inv, tcfitr.gen.costs.var, tcfitr.general.fin, tcfitr.general, tcfitr.general.hours, tcfitr.general.result, tcfitr.gen.costs.wo, tcfitr.gen.cogs.trian, tcfitr.interim.result, tcfitr.internal.servic, tcfitr.issue, tcfitr.wip.balance, tcfitr.issue.invoiced, tcfitr.rev.wip.balance, tcfitr.item.surch.rec, tcfitr.item.surch.iss, tcfitr.cons.reject, tcfitr.cons.issue, tcfitr.on.order, tcfitr.operation, tcfitr.order.disc, tcfitr.cons.receipt, tcfitr.cons.use, tcfitr.price.variance, tcfitr.trans.serv.wip, tcfitr.ftp.result, tcfitr.lot.result, tcfitr.project.surch, tcfitr.quarantine.rec, tcfitr.pur.price.var, tcfitr.rebate.res, tcfitr.rebate, tcfitr.receipt, tcfitr.rejection, tcfitr.deferred.rev, tcfitr.wip.revaluation, tcfitr.revaluation, tcfitr.revenue.finance, tcfitr.cons.reval, tcfitr.revenues, tcfitr.revenues.anal, tcfitr.cons.return, tcfitr.rev.interim.res, tcfitr.cons.issue.res, tcfitr.receip.invoiced, tcfitr.shipm.var.inv, tcfitr.shipm.variance, tcfitr.subcontracting, tcfitr.return.result, tcfitr.subcontr.wip, tcfitr.reject.invoiced, tcfitr.adv.inst.paid, tcfitr.advance.install, tcfitr.normal.install, tcfitr.settl.guar.inst, tcfitr.subcontr.inv, tcfitr.operation.inv, tcfitr.gen.cost.wo.inv, tcfitr.ship.var.inv.pr, tcfitr.gen.costs.inv, tcfitr.trian.invoiced, tcfitr.return.res.inv, tcfitr.trans.msc.inv, tcfitr.trans.accr, tcfitr.trans.msc, tcfitr.material, tcfitr.mat.invoiced, tcfitr.value.correct, tcfitr.repair.warranty, tcfitr.wip.hours, tcfitr.warranty.costs, tcfitr.wip.costs, tcfitr.wip.cst.finance, tcfitr.trans.proj.wip, tcfitr.wip.tran.issue, tcfitr.wip.tran.receip, tcfitr.wip.tran.issinv, tcfitr.wip.tran.recinv, tcfitr.warh.surch.iss, tcfitr.warh.surch.rec, tcfitr.holdback, tcfitr.wip.var.adjust, tcfitr.wip.value.corr, tcfitr.cogs, tcfitr.goodwill, tcfitr.tool.costs, tcfitr.tool.costs.inv, tcfitr.issue.inv.proj, tcfitr.ln.cost, tcfitr.ln.cost.var, tcfitr.ln.cost.inv, tcfitr.ln.cost.inv.var, tcfitr.invoiced.wip, tcfitr.issue.proj, tcfitr.appr.inv.proj, tcfitr.inv.value.corr, tcfitr.loan.result, tcfitr.so.price.var, tcfitr.cogs.int.inv, tcfitr.int.inv, tcfitr.plan.st.paym, tcfitr.rel.st.paym, tcfitr.receipt.st.paym, tcfitr.loan, tcfitr.borrow, tcfitr.payback.result, tcfitr.rec.var.st.paym, tcfitr.proj.rec.var.sp, tcfitr.cost.peg.trf.sp, tcfitr.curr.var.sp, tcfitr.approval, tcfitr.wip.trf.iss.ord, tcfitr.wip.trf.rec.ord, tcfitr.rec.var.cst.spc, tcfitr.exp.tax.st.paym, tcfitr.loan.reversal, tcfitr.borrow.reversal, tcfitr.curr.var, tcfitr.quarantine.iss, tcfitr.wip.quar.iss, tcfitr.wip.quar.rec, tcfitr.quarantine.adj, tcfitr.wip.quar.adj, tcfitr.quarantine.rev, tcfitr.wip.quar.rev, tcfitr.curr.var.lc, tcfitr.progr.payment, tcfitr.rev.interc, tcfitr.work.cell.sur, tcfitr.wip.repair.iss, tcfitr.wip.costs.ic, tcfitr.wip.hours.ic, tcfitr.gen.hours.ic, tcfitr.expenses.ic, tcfitr.costs.ic.hours, tcfitr.costs.ic.exp, tcfitr.issue.res.proj, tcfitr.ln.cost.ic, tcfitr.ln.cost.inv.ic, tcfitr.rec.ic.st.paym, tcfitr.dir.rec.ic, tcfitr.variance.ic, tcfitr.costs.ic.pur, tcfitr.cogs.variance, tcfitr.cogs.quot.var, tcfitr.cogs.reval, tcfitr.cons.var.adj, tcfitr.cons.val.corr, tcfitr.cos.inv, tcfitr.cos.rec, tcfitr.cos.quot.inv, tcfitr.cos.quot.rec, tcfitr.rev.inv, tcfitr.rev.rec, tcfitr.ord.disc.inv, tcfitr.ord.disc.rec, tcfitr.cos.inv.proj, tcfitr.rev.inv.proj, tcfitr.ord.disc.inv.pr, tcfitr.afp.commit, tcfitr.rental, tcfitr.rental.ic, tcfitr.costs.ic.rental, tcfitr.tr.serv.wip.ic, tcfitr.tr.proj.wip.ic, tcfitr.costs.ic.serv, tcfitr.rent.sh.var.ic, tcfitr.ftp.result.proj, tcfitr.measure.result, tcfitr.rental.cos, tcfitr.rent.rev.anal, tcfitr.rent.rev, tcfitr.not.applicable |
| `potf` | `integer` | `int` |  |  | Post to Finance. Allowed values: 1, 2 |
| `potf_kw` | `string` | `varchar` |  |  | Post to Finance (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `intc` | `integer` | `int` |  |  | Intercompany. Allowed values: 1, 2 |
| `intc_kw` | `string` | `varchar` |  |  | Intercompany (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `csts` | `integer` | `int` |  |  | Cost Transaction. Allowed values: 1, 2 |
| `csts_kw` | `string` | `varchar` |  |  | Cost Transaction (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oldf` | `integer` | `int` |  |  | Old-fashioned Flow. Allowed values: 1, 2 |
| `oldf_kw` | `string` | `varchar` |  |  | Old-fashioned Flow (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bwln` | `integer` | `int` |  |  | Borrow/Loan. Allowed values: 1, 2 |
| `bwln_kw` | `string` | `varchar` |  |  | Borrow/Loan (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inti` | `integer` | `int` |  |  | Intercompany Trade. Allowed values: 1, 2 |
| `inti_kw` | `string` | `varchar` |  |  | Intercompany Trade (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `retx` | `integer` | `int` |  |  | Receipt Expense Tax. Allowed values: 1, 2 |
| `retx_kw` | `string` | `varchar` |  |  | Receipt Expense Tax (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `comf` | `integer` | `int` |  |  | Reported Complete (Finance). Allowed values: 1, 2, 3 |
| `comf_kw` | `string` | `varchar` |  |  | Reported Complete (Finance) (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `pstt` | `integer` | `int` |  |  | Posted to Import/Export Statistics. Allowed values: 1, 2 |
| `pstt_kw` | `string` | `varchar` |  |  | Posted to Import/Export Statistics (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `snec` | `integer` | `int` |  |  | Commitment Sequence |
| `sttl` | `integer` | `int` |  |  | Settlement Origin. Allowed values: 1, 2 |
| `sttl_kw` | `string` | `varchar` |  |  | Settlement Origin (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `alin` | `integer` | `int` |  |  | Allowed for Invoicing. Allowed values: 1, 2 |
| `alin_kw` | `string` | `varchar` |  |  | Allowed for Invoicing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ntbl` | `integer` | `int` |  |  | Suppressed for Next Billing Cycle. Allowed values: 1, 2 |
| `ntbl_kw` | `string` | `varchar` |  |  | Suppressed for Next Billing Cycle (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pris` | `float` | `float` |  |  | Sales Price |
| `amos` | `float` | `float` |  |  | Sales Amount |
| `curs` | `string` | `varchar` |  |  | Sales Currency. Max length: 3 |
| `rtcs_1` | `float` | `float` |  |  | Currency Rate (Sales) |
| `rtcs_2` | `float` | `float` |  |  | Currency Rate (Sales) |
| `rtcs_3` | `float` | `float` |  |  | Currency Rate (Sales) |
| `rtfs_1` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_2` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_3` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rdas` | `timestamp` | `timestamp_ntz` |  |  | Rate Date (Sales) |
| `invo` | `integer` | `int` |  |  | Approved for Invoicing. Allowed values: 1, 2 |
| `invo_kw` | `string` | `varchar` |  |  | Approved for Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `trsl` | `integer` | `int` |  |  | Transferred to Invoicing. Allowed values: 1, 2 |
| `trsl_kw` | `string` | `varchar` |  |  | Transferred to Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `isln` | `integer` | `int` |  |  | Sales Invoice Line |
| `hbyn` | `integer` | `int` |  |  | Use Holdback. Allowed values: 1, 2 |
| `hbyn_kw` | `string` | `varchar` |  |  | Use Holdback (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `hbnr` | `integer` | `int` |  |  | Holdback Sequence Number |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txct` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bpct` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `rcod` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `ecom` | `integer` | `int` |  |  | Employee Company |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `chpr` | `float` | `float` |  |  | Hours Surcharge % |
| `cwgt` | `string` | `varchar` |  |  | Labor Type. Max length: 3 |
| `serh` | `integer` | `int` |  |  | Hours Accounting Sequence |
| `cpth` | `string` | `varchar` |  |  | Period Table Hours Accounting. Max length: 6 |
| `hyea` | `integer` | `int` |  |  | Year Hours Accounting |
| `hper` | `integer` | `int` |  |  | Period Hours Accounting |
| `base` | `string` | `varchar` |  |  | Overhead Base. Max length: 8 |
| `vrsn` | `integer` | `int` |  |  | Overhead Base Version |
| `ohpr` | `float` | `float` |  |  | Overhead Percentage |
| `bprc` | `float` | `float` |  |  | Billing Percentage |
| `itco` | `integer` | `int` |  |  | Intercompany Trade Company |
| `itor` | `string` | `varchar` |  |  | Intercompany Trade Order. Max length: 9 |
| `itln` | `integer` | `int` |  |  | Intercompany Trade Order Line |
| `reas` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `ccma` | `string` | `varchar` |  |  | Cost Component for Mapping. Max length: 8 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `task` | `string` | `varchar` |  |  | Labor. Max length: 8 |
| `cequ` | `string` | `varchar` |  |  | Equipment. Max length: 47 |
| `csub` | `string` | `varchar` |  |  | Subcontracting. Max length: 47 |
| `cico` | `string` | `varchar` |  |  | Sundry Cost. Max length: 8 |
| `ovhd` | `string` | `varchar` |  |  | Overhead. Max length: 8 |
| `oitm` | `string` | `varchar` |  |  | Original Item. Max length: 47 |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `itgd` | `string` | `varchar` |  |  | Integration Transaction GUID. Max length: 22 |
| `xgui` | `string` | `varchar` |  |  | Expense Report GUID. Max length: 22 |
| `guid` | `string` | `varchar` |  |  | Unique ID. Max length: 22 |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `txta` | `integer` | `int` |  |  | Cost Text |
| `btxt` | `integer` | `int` |  |  | Billing Comment |
| `cdf_sref__en_us` | `string` | `varchar` |  | `not_null` | (required) SAP Reference - base language. Max length: 30 |
| `cdf_susr__en_us` | `string` | `varchar` |  | `not_null` | (required) SAP User - base language. Max length: 30 |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `phit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `efph_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `curc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cdoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin105 Invoice Text by Document |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cptf_fyea_fper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `drun_serb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpppc005 Processing Runs |
| `cdru_cser_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpppc005 Processing Runs |
| `curs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `txct_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `txct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `bpct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
| `cwgt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl030 Labor Types |
| `base_vrsn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm200 Overhead Application Base |
| `reas_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ccma_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `ovhd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm042 Standard Overhead |
| `oitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `btxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `amoc_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `amos_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `amoc_prjc` | `float` | `float` |  |  | CC: Cost Amount in Project Currency |
| `amos_prjc` | `float` | `float` |  |  | CC: Sales Amount in Project Currency |
| `amoc_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `amos_cntc` | `float` | `float` |  |  | CC: Sales Amount in Contract Currency |
| `amoc_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
| `amos_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
| `amos_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `amos_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `amos_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `quan_buar` | `float` | `float` |  |  | CC: Quantity in Base Unit Area |
| `quan_buvl` | `float` | `float` |  |  | CC: Quantity in Base Unit Volume |
| `quan_buln` | `float` | `float` |  |  | CC: Quantity in Base Unit Length |
| `quan_bupc` | `float` | `float` |  |  | CC: Quantity in Base Unit Piece |
| `quan_buwg` | `float` | `float` |  |  | CC: Quantity in Base Unit Weight |
| `quan_base_time` | `float` | `float` |  |  | CC: Labor hours in General Unit of Hours |
| `quan_invu` | `float` | `float` |  |  | CC: Quantity in Inventory Unit |
| `comm_acts` | `string` | `varchar` |  |  | CC: Commitment Type. Max length: 15 |
| `cprj_task` | `string` | `varchar` |  |  | CC: Project Labor. Max length: 25 |
| `cprj_csub` | `string` | `varchar` |  |  | CC: Project Subcontracing. Max length: 60 |
| `cprj_cequ` | `string` | `varchar` |  |  | CC: Project Equipment. Max length: 60 |
| `cprj_cico` | `string` | `varchar` |  |  | CC: Project Sundry. Max length: 25 |
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

# tfacp240

LN tfacp240 Order Data for Approval table, Generated 2026-04-10T19:41:28Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp240` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `loco`, `otyp`, `orno`, `pono`, `sqnb` |
| **Column count** | 146 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `loco` | `integer` | `int` | `PK` | `not_null` | (required) Logistic Company |
| `otyp` | `integer` | `int` | `PK` | `not_null` | (required) Order Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 |
| `otyp_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tfacp.otyp.purchase, tfacp.otyp.sales, tfacp.otyp.trans, tfacp.otyp.trans.man, tfacp.otyp.trans.wip, tfacp.otyp.freight, tfacp.otyp.commissions, tfacp.otyp.project, tfacp.otyp.project.man, tfacp.otyp.ent.planning, tfacp.otyp.assembly, tfacp.otyp.production, tfacp.otyp.service, tfacp.otyp.maint.sales, tfacp.otyp.pcs.project, tfacp.otyp.maint.work, tfacp.otyp.not.applicable, tfacp.otyp.proj.contract, tfacp.otyp.customer.claim, tfacp.otyp.intercomp.trade, tfacp.otyp.services.procm |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Position |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `fcpo` | `integer` | `int` |  |  | Financial Company Purchase Office |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `koor` | `integer` | `int` |  |  | Type of Purchase order. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `koor_kw` | `string` | `varchar` |  |  | Type of Purchase order (keyword). Allowed values: tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship- from address. Max length: 9 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `proj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `bppr` | `integer` | `int` |  |  | Infor LN Project. Allowed values: 1, 2 |
| `bppr_kw` | `string` | `varchar` |  |  | Infor LN Project (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `itgr` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `glco` | `string` | `varchar` |  |  | GL Code. Max length: 135 |
| `poff` | `string` | `varchar` |  |  | Invoice-from Department. Max length: 6 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vatc` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | Business Partner's Tax Country. Max length: 3 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `mcfr` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `mcfr_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `iuni` | `string` | `varchar` |  |  | Inventory Unit. Max length: 3 |
| `pcun` | `string` | `varchar` |  |  | Piece Unit. Max length: 3 |
| `unit` | `string` | `varchar` |  |  | Purchase Unit. Max length: 3 |
| `oqan` | `float` | `float` |  |  | Order Quantity in inventory unit |
| `oqpc` | `float` | `float` |  |  | Order Quantity in Piece Unit |
| `oamt` | `float` | `float` |  |  | Order Amount in Order Currency |
| `amth_1` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_2` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_3` | `float` | `float` |  |  | Amount in Home Currency |
| `rqty` | `float` | `float` |  |  | Total Goods Received Quantity in Inventory Unit |
| `rqpc` | `float` | `float` |  |  | Total Received Quantity in Piece Unit |
| `tapq` | `float` | `float` |  |  | Total Approved Goods Received Quantity in Inventory Unit |
| `aqpc` | `float` | `float` |  |  | Total Approved Quantity in Piece Unit |
| `trqu` | `float` | `float` |  |  | Total Rejected Quantity in Inventory Unit (Goods) |
| `rjpc` | `float` | `float` |  |  | Total Rejected Quantity in Piece Unit |
| `fire` | `integer` | `int` |  |  | Final goods receipt. Allowed values: 1, 2 |
| `fire_kw` | `string` | `varchar` |  |  | Final goods receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `byer` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `ladt` | `timestamp` | `timestamp_ntz` |  |  | Latest Good Rcpt Date |
| `toma` | `integer` | `int` |  |  | Total Matched. Allowed values: 1, 2 |
| `toma_kw` | `string` | `varchar` |  |  | Total Matched (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sfbl` | `integer` | `int` |  |  | Self-billed Order. Allowed values: 1, 2 |
| `sfbl_kw` | `string` | `varchar` |  |  | Self-billed Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `paft` | `integer` | `int` |  |  | Pay after approval. Allowed values: 1, 2 |
| `paft_kw` | `string` | `varchar` |  |  | Pay after approval (keyword). Allowed values: tcpaft.approval, tcpaft.receipts |
| `rcod` | `string` | `varchar` |  |  | Exempt Reason. Max length: 6 |
| `ceno` | `string` | `varchar` |  |  | Tax Exemption Certificate Number. Max length: 24 |
| `ppvp` | `integer` | `int` |  |  | Purchase Price Variance Posted. Allowed values: 1, 2 |
| `ppvp_kw` | `string` | `varchar` |  |  | Purchase Price Variance Posted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ptyp` | `string` | `varchar` |  |  | Purchase Type. Max length: 6 |
| `fsli` | `integer` | `int` |  |  | Financial Company Central Invoicing |
| `slcp` | `integer` | `int` |  |  | Source Company Central Invoicing |
| `srtp` | `integer` | `int` |  |  | Source Type Central Invoicing. Allowed values: 10, 20, 30, 34, 40, 43, 44, 48, 52, 60, 64, 66, 68, 70, 72, 80, 90, 100, 110, 120, 130, 190, 200 |
| `srtp_kw` | `string` | `varchar` |  |  | Source Type Central Invoicing (keyword). Allowed values: tcsli.srtp.interest, tcsli.srtp.manual.sales, tcsli.srtp.project.contr, tcsli.srtp.project, tcsli.srtp.sales.order, tcsli.srtp.int.trade.order, tcsli.srtp.wareh.order, tcsli.srtp.purchase.order, tcsli.srtp.pcs.order, tcsli.srtp.service.order, tcsli.srtp.maint.sales.ord, tcsli.srtp.maint.wrk.ord, tcsli.srtp.service.call, tcsli.srtp.customer.claim, tcsli.srtp.supplier.claim, tcsli.srtp.service.contr, tcsli.srtp.freight, tcsli.srtp.debit.credit, tcsli.srtp.rebate, tcsli.srtp.shipment, tcsli.srtp.receive.sls.inv, tcsli.srtp.all, tcsli.srtp.not.appl |
| `slso` | `string` | `varchar` |  |  | Order Number Central Invoicing. Max length: 9 |
| `cipo` | `integer` | `int` |  |  | Order Line Central Invoicing |
| `oref` | `string` | `varchar` |  |  | Order Reference Central Invoicing. Max length: 9 |
| `tref` | `string` | `varchar` |  |  | Technical Reference Central Invoicing. Max length: 70 |
| `ciko` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `ciko_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `cisq` | `integer` | `int` |  |  | Obsolete |
| `ortp` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9 |
| `ortp_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: cisli.ortp.sales, cisli.ortp.warehouse, cisli.ortp.serv.ord, cisli.ortp.serv.call, cisli.ortp.maint.sales.ord, cisli.ortp.purchase, cisli.ortp.rebate, cisli.ortp.not.app, cisli.ortp.pcs.order |
| `oset` | `integer` | `int` |  |  | Obsolete |
| `cish` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `paya` | `string` | `varchar` |  |  | Payment Agreement. Max length: 3 |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `mcpr` | `integer` | `int` |  |  | Multiple Consumptions per Goods Receipt. Allowed values: 1, 2 |
| `mcpr_kw` | `string` | `varchar` |  |  | Multiple Consumptions per Goods Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `damt` | `float` | `float` |  |  | Total Discount Amount in Order Currency |
| `ityp` | `integer` | `int` |  |  | Invoicing Type. Allowed values: 1, 2, 3 |
| `ityp_kw` | `string` | `varchar` |  |  | Invoicing Type (keyword). Allowed values: tfacp.ityp.manual, tfacp.ityp.self.billing, tfacp.ityp.internal |
| `itsc` | `integer` | `int` |  |  | Intercompany Trade Scenario. Allowed values: 10, 15, 20, 30, 40, 60, 65, 80, 85, 90, 95, 98, 102, 103, 108, 110, 130, 150, 200 |
| `itsc_kw` | `string` | `varchar` |  |  | Intercompany Trade Scenario (keyword). Allowed values: tcitr.scen.ext.mat.del.sls, tcitr.scen.ext.mat.del.pur, tcitr.scen.ext.mat.dir.del, tcitr.scen.int.mat.del, tcitr.scen.freight, tcitr.scen.subc.dep.rep, tcitr.scen.int.srv.del, tcitr.scen.labor, tcitr.scen.expenses, tcitr.scen.other, tcitr.scen.subcon, tcitr.scen.tool, tcitr.scen.trav.time, tcitr.scen.trav.other, tcitr.scen.helpdesk, tcitr.scen.rental, tcitr.scen.pcs.del, tcitr.scen.wip.transfer, tcitr.scen.not.appl |
| `sorn__en_us` | `string` | `varchar` |  | `not_null` | (required) Buy-from Business Partner Order - base language. Max length: 30 |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `crrf` | `integer` | `int` |  |  | Item Cross Reference. Allowed values: 0, 1, 2, 3 |
| `crrf_kw` | `string` | `varchar` |  |  | Item Cross Reference (keyword). Allowed values: , tccrrf.ics, tccrrf.mpn, tccrrf.na |
| `citt` | `string` | `varchar` |  |  | Code Item Type. Max length: 3 |
| `crit__en_us` | `string` | `varchar` |  | `not_null` | (required) Cross Reference Item - base language. Max length: 35 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `prrc` | `integer` | `int` |  |  | Price per Goods Receipt. Allowed values: 1, 2 |
| `prrc_kw` | `string` | `varchar` |  |  | Price per Goods Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `issp` | `integer` | `int` |  |  | Invoice by Stage Payment. Allowed values: 1, 2 |
| `issp_kw` | `string` | `varchar` |  |  | Invoice by Stage Payment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mtch` | `integer` | `int` |  |  | Match. Allowed values: 1, 2 |
| `mtch_kw` | `string` | `varchar` |  |  | Match (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `maqu` | `float` | `float` |  |  | Match Quantity |
| `mqpc` | `float` | `float` |  |  | Match Quantity in Piece Unit |
| `mqpu` | `float` | `float` |  |  | Match Quantity in Purchase Unit |
| `maam` | `float` | `float` |  |  | Match Amount |
| `lmat` | `integer` | `int` |  |  | Level of Match. Allowed values: 10, 20 |
| `lmat_kw` | `string` | `varchar` |  |  | Level of Match (keyword). Allowed values: tfacp.lmat.inv.header, tfacp.lmat.inv.line |
| `omti` | `integer` | `int` |  |  | Overwrite Match of Invoice Header. Allowed values: 1, 2 |
| `omti_kw` | `string` | `varchar` |  |  | Overwrite Match of Invoice Header (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uppr` | `integer` | `int` |  |  | Update Average/Last Purchase Price. Allowed values: 1, 2 |
| `uppr_kw` | `string` | `varchar` |  |  | Update Average/Last Purchase Price (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clas` | `string` | `varchar` |  |  | Classification Scheme. Max length: 9 |
| `clsc` | `string` | `varchar` |  |  | Classification Scheme Code. Max length: 25 |
| `clsb` | `integer` | `int` |  |  | Classification Scheme Base. Allowed values: 10, 20, 100 |
| `clsb_kw` | `string` | `varchar` |  |  | Classification Scheme Base (keyword). Allowed values: tctax.vtbo.services, tctax.vtbo.goods, tctax.vtbo.not.appl |
| `trgu` | `string` | `varchar` |  |  | GUID. Max length: 22 |
| `cvlo` | `float` | `float` |  |  | Customs Value in Order Currency |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `iuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `unit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ptyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs201 Purchase Types |
| `paya_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs206 Payment Agreements |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `clas_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccls010 Classification Schemes |
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

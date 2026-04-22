# tisfc001

LN tisfc001 Production Orders table, Generated 2026-04-10T19:41:50Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tisfc001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pdno` |
| **Column count** | 241 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pdno` | `string` | `varchar` | `PK` | `not_null` | (required) Production Order. Max length: 9 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `mitm` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `qrdr` | `float` | `float` |  |  | Quantity Ordered |
| `rdld` | `timestamp` | `timestamp_ntz` |  |  | Requested Delivery Date |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `revi` | `string` | `varchar` |  |  | Engineering Item Revision. Max length: 6 |
| `rwko` | `integer` | `int` |  |  | Rework Order. Allowed values: 1, 2 |
| `rwko_kw` | `string` | `varchar` |  |  | Rework Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rwtp` | `integer` | `int` |  |  | Rework Type. Allowed values: 10, 20, 30, 40, 50, 240 |
| `rwtp_kw` | `string` | `varchar` |  |  | Rework Type (keyword). Allowed values: tisfc.rwtp.not.appl, tisfc.rwtp.manual, tisfc.rwtp.quar.t.exist, tisfc.rwtp.quar.t.new, tisfc.rwtp.refurbish.tool, tisfc.rwtp.disassembly |
| `opro` | `string` | `varchar` |  |  | Routing. Max length: 6 |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Reference Date |
| `rgrp` | `string` | `varchar` |  |  | Routing Group. Max length: 6 |
| `mfre` | `integer` | `int` |  |  | Moment Freezing Estimates. Allowed values: 1, 2, 3 |
| `mfre_kw` | `string` | `varchar` |  |  | Moment Freezing Estimates (keyword). Allowed values: tisfc.mfre.at.creation, tisfc.mfre.at.release, tisfc.mfre.bef.first.wipt |
| `shsp` | `integer` | `int` |  |  | Split Hours in Setup and Production. Allowed values: 1, 2 |
| `shsp_kw` | `string` | `varchar` |  |  | Split Hours in Setup and Production (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prcd` | `integer` | `int` |  |  | Priority |
| `prlb` | `integer` | `int` |  |  | Print Labels. Allowed values: 1, 2 |
| `prlb_kw` | `string` | `varchar` |  |  | Print Labels (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lbtp` | `integer` | `int` |  |  | Label Type. Allowed values: 10, 20, 100 |
| `lbtp_kw` | `string` | `varchar` |  |  | Label Type (keyword). Allowed values: tirou.lbtp.internal, tirou.lbtp.customer, tirou.lbtp.not.appl |
| `lbpr` | `integer` | `int` |  |  | Procedure. Allowed values: 10, 20, 30, 100 |
| `lbpr_kw` | `string` | `varchar` |  |  | Procedure (keyword). Allowed values: tirou.lbpr.scan.book, tirou.lbpr.wh.receipt, tirou.lbpr.rep.order, tirou.lbpr.not.appl |
| `lbpb` | `integer` | `int` |  |  | Label Printed By. Allowed values: 10, 20, 100 |
| `lbpb_kw` | `string` | `varchar` |  |  | Label Printed By (keyword). Allowed values: tirou.lbpb.internal, tirou.lbpb.external, tirou.lbpb.not.appl |
| `lblo` | `string` | `varchar` |  |  | Label Layout. Max length: 3 |
| `lbdv` | `string` | `varchar` |  |  | Label Device. Max length: 14 |
| `lbnc` | `integer` | `int` |  |  | Number of Copies |
| `aapd` | `integer` | `int` |  |  | Allow Alternative Package Definition. Allowed values: 1, 2 |
| `aapd_kw` | `string` | `varchar` |  |  | Allow Alternative Package Definition (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pkdf` | `string` | `varchar` |  |  | Package Definition. Max length: 6 |
| `clpd` | `integer` | `int` |  |  | Customer Labels Printed. Allowed values: 10, 20, 30 |
| `clpd_kw` | `string` | `varchar` |  |  | Customer Labels Printed (keyword). Allowed values: tisfc.clpd.no, tisfc.clpd.partial, tisfc.clpd.yes |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `huqt` | `float` | `float` |  |  | Handling Unit Quantity |
| `bfep` | `integer` | `int` |  |  | Backflush Materials. Allowed values: 1, 2 |
| `bfep_kw` | `string` | `varchar` |  |  | Backflush Materials (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bfhr` | `integer` | `int` |  |  | Backflush Hours. Allowed values: 1, 2 |
| `bfhr_kw` | `string` | `varchar` |  |  | Backflush Hours (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rswc` | `integer` | `int` |  |  | Financial Transactions by Work Center. Allowed values: 0, 1, 2 |
| `rswc_kw` | `string` | `varchar` |  |  | Financial Transactions by Work Center (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `trcp` | `string` | `varchar` |  |  | Obsolete. Max length: 8 |
| `clco` | `string` | `varchar` |  |  | Calculation Office. Max length: 6 |
| `cctt` | `integer` | `int` |  |  | Update Method. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 18, 20, 21, 23, 24, 25, 26, 30, 31, 32, 33, 34, 35, 40, 45, 47 |
| `cctt_kw` | `string` | `varchar` |  |  | Update Method (keyword). Allowed values: tisfc.updm.generate, tisfc.updm.recalc, tisfc.updm.regen, tisfc.updm.doc.printed, tisfc.updm.release, tisfc.updm.activate, tisfc.updm.rep.compl, tisfc.updm.close, tisfc.updm.archive, tisfc.updm.no.action, tisfc.updm.schedule, tisfc.updm.wip.printed, tisfc.updm.act.cst.printed, tisfc.updm.res.wc.printed, tisfc.updm.tim.dev.printed, tisfc.updm.update, tisfc.updm.cancel, tisfc.updm.set.def.close, tisfc.updm.set.oper.pres, tisfc.updm.set.dist.pres, tisfc.updm.set.prt.label, tisfc.updm.reset.all.docs, tisfc.updm.reset.mat.docs, tisfc.updm.reset.ser.list, tisfc.updm.reset.sub.note, tisfc.updm.reset.stat.clos, tisfc.updm.reset.stat.comp, tisfc.updm.split.order, tisfc.updm.prod.sched, tisfc.updm.sched.plus |
| `grid` | `string` | `varchar` |  |  | Production Order Group. Max length: 9 |
| `osta` | `integer` | `int` |  |  | Order Status. Allowed values: 1, 2, 3, 4, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110 |
| `osta_kw` | `string` | `varchar` |  |  | Order Status (keyword). Allowed values: tcosta.old.planned, tcosta.old.scheduled, tcosta.old.printed, tcosta.old.in.prod, tcosta.old.active, tcosta.old.to.be.compl, tcosta.old.completed, tcosta.old.closed, tcosta.old.archived, tcosta.planned, tcosta.modify, tcosta.scheduled, tcosta.printed, tcosta.in.prod, tcosta.active, tcosta.to.be.completed, tcosta.completed, tcosta.closed, tcosta.archived |
| `vers` | `integer` | `int` |  |  | Version |
| `cdld` | `timestamp` | `timestamp_ntz` |  |  | Confirmed Delivery Date |
| `cada` | `timestamp` | `timestamp_ntz` |  |  | Calculation Date |
| `fres` | `integer` | `int` |  |  | Estimated Costs Frozen. Allowed values: 1, 2 |
| `fres_kw` | `string` | `varchar` |  |  | Estimated Costs Frozen (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clot` | `string` | `varchar` |  |  | Lot Code. Max length: 20 |
| `lbid` | `string` | `varchar` |  |  | Obsolete. Max length: 25 |
| `defc` | `integer` | `int` |  |  | Definitively Closed. Allowed values: 1, 2 |
| `defc_kw` | `string` | `varchar` |  |  | Definitively Closed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bloc` | `integer` | `int` |  |  | Operations Blocked. Allowed values: 1, 2 |
| `bloc_kw` | `string` | `varchar` |  |  | Operations Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oprp` | `integer` | `int` |  |  | Operations Present. Allowed values: 0, 1, 2 |
| `oprp_kw` | `string` | `varchar` |  |  | Operations Present (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `dstp` | `integer` | `int` |  |  | Distribution Type. Allowed values: 10, 20, 30, 40, 50, 60 |
| `dstp_kw` | `string` | `varchar` |  |  | Distribution Type (keyword). Allowed values: tcdstp.not.appl, tcdstp.units, tcdstp.pegs, tcdstp.units.pegs, tcdstp.cost.pegs, tcdstp.units.cost.pegs |
| `odpr` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `odpr_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpla` | `integer` | `int` |  |  | Planning Method. Allowed values: 1, 2 |
| `cpla_kw` | `string` | `varchar` |  |  | Planning Method (keyword). Allowed values: tcplcd.forward, tcplcd.backward |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Production Start Date and Time |
| `pldt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `mmno` | `integer` | `int` |  |  | Moving Method Next Operations. Allowed values: 1, 2, 3, 4, 10 |
| `mmno_kw` | `string` | `varchar` |  |  | Moving Method Next Operations (keyword). Allowed values: tisfc.mmop.not, tisfc.mmop.all, tisfc.mmop.overlap.occ, tisfc.mmop.overlap.hundred, tisfc.mmop.dynamic |
| `mmpo` | `integer` | `int` |  |  | Moving Method Previous Operations. Allowed values: 1, 2, 3, 4, 10 |
| `mmpo_kw` | `string` | `varchar` |  |  | Moving Method Previous Operations (keyword). Allowed values: tisfc.mmop.not, tisfc.mmop.all, tisfc.mmop.overlap.occ, tisfc.mmop.overlap.hundred, tisfc.mmop.dynamic |
| `plid` | `string` | `varchar` |  |  | Planner. Max length: 9 |
| `sfpl` | `string` | `varchar` |  |  | Shop Floor Planner. Max length: 9 |
| `apdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Production Start Date |
| `cmdt` | `timestamp` | `timestamp_ntz` |  |  | Completion Date |
| `adld` | `timestamp` | `timestamp_ntz` |  |  | Actual Delivery Date |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Actual Closing date |
| `qntl` | `float` | `float` |  |  | Quantity Initial |
| `qtdl` | `float` | `float` |  |  | Quantity to Deliver |
| `qdlv` | `float` | `float` |  |  | Quantity Delivered |
| `qrjc` | `float` | `float` |  |  | Quantity Scrapped |
| `qtor` | `float` | `float` |  |  | Quantity Rejected |
| `qqar` | `float` | `float` |  |  | Quantity Quarantined |
| `qtbf` | `float` | `float` |  |  | Quantity to Backflush |
| `qbfd` | `float` | `float` |  |  | Quantity Backflushed |
| `qoor` | `float` | `float` |  |  | Obsolete |
| `covn` | `integer` | `int` |  |  | Covering Note Printed. Allowed values: 1, 2, 3 |
| `covn_kw` | `string` | `varchar` |  |  | Covering Note Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `roul` | `integer` | `int` |  |  | Routing Sheet Printed. Allowed values: 1, 2, 3 |
| `roul_kw` | `string` | `varchar` |  |  | Routing Sheet Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `oprn` | `integer` | `int` |  |  | Operation Note Printed. Allowed values: 1, 2, 3 |
| `oprn_kw` | `string` | `varchar` |  |  | Operation Note Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `matl` | `integer` | `int` |  |  | Material List Printed. Allowed values: 1, 2, 3 |
| `matl_kw` | `string` | `varchar` |  |  | Material List Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `matn` | `integer` | `int` |  |  | Material Issue Note Printed. Allowed values: 1, 2, 3 |
| `matn_kw` | `string` | `varchar` |  |  | Material Issue Note Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `subn` | `integer` | `int` |  |  | Subcontracting Note Printed. Allowed values: 1, 2, 3 |
| `subn_kw` | `string` | `varchar` |  |  | Subcontracting Note Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `chel` | `integer` | `int` |  |  | Checklist Printed. Allowed values: 1, 2, 3 |
| `chel_kw` | `string` | `varchar` |  |  | Checklist Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `sawl` | `integer` | `int` |  |  | Sawing List Printed. Allowed values: 1, 2, 3 |
| `sawl_kw` | `string` | `varchar` |  |  | Sawing List Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `cutl` | `integer` | `int` |  |  | Cutting List Printed. Allowed values: 1, 2, 3 |
| `cutl_kw` | `string` | `varchar` |  |  | Cutting List Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `recn` | `integer` | `int` |  |  | Receipt Note Printed. Allowed values: 1, 2, 3 |
| `recn_kw` | `string` | `varchar` |  |  | Receipt Note Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `insn` | `integer` | `int` |  |  | Inspection Note Printed. Allowed values: 1, 2, 3 |
| `insn_kw` | `string` | `varchar` |  |  | Inspection Note Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `srnl` | `integer` | `int` |  |  | Serial Number List Printed. Allowed values: 1, 2, 3 |
| `srnl_kw` | `string` | `varchar` |  |  | Serial Number List Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `odpt` | `integer` | `int` |  |  | Order Distribution Printed. Allowed values: 1, 2, 3 |
| `odpt_kw` | `string` | `varchar` |  |  | Order Distribution Printed (keyword). Allowed values: tisfc.pdst.original.doc, tisfc.pdst.modified.doc, tisfc.pdst.doc.printed |
| `crpr_1` | `integer` | `int` |  |  | Costing Reports Printed. Allowed values: 1, 2 |
| `crpr_kw_1` | `string` | `varchar` |  |  | Costing Reports Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crpr_2` | `integer` | `int` |  |  | Costing Reports Printed. Allowed values: 1, 2 |
| `crpr_kw_2` | `string` | `varchar` |  |  | Costing Reports Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crpr_3` | `integer` | `int` |  |  | Costing Reports Printed. Allowed values: 1, 2 |
| `crpr_kw_3` | `string` | `varchar` |  |  | Costing Reports Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crpr_4` | `integer` | `int` |  |  | Costing Reports Printed. Allowed values: 1, 2 |
| `crpr_kw_4` | `string` | `varchar` |  |  | Costing Reports Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aprp` | `integer` | `int` |  |  | Use Actual Cost for Receipt Posting. Allowed values: 1, 2 |
| `aprp_kw` | `string` | `varchar` |  |  | Use Actual Cost for Receipt Posting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spcn` | `string` | `varchar` |  |  | Specifications Number. Max length: 22 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `subc` | `integer` | `int` |  |  | Contains Customer Furnished Material. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Contains Customer Furnished Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `owns` | `integer` | `int` |  |  | Obsolete. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `mocp` | `integer` | `int` |  |  | Moment of Completion Posting. Allowed values: 2, 4 |
| `mocp_kw` | `string` | `varchar` |  |  | Moment of Completion Posting (keyword). Allowed values: tisfc.mocp.qty.compl, tisfc.mocp.osta.compl |
| `reco` | `string` | `varchar` |  |  | Reason for Rejects. Max length: 6 |
| `poas` | `string` | `varchar` |  |  | Parent Order. Max length: 9 |
| `smfs` | `integer` | `int` |  |  | Subcontracting with Material Flow. Allowed values: 1, 2 |
| `smfs_kw` | `string` | `varchar` |  |  | Subcontracting with Material Flow (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asrp` | `integer` | `int` |  |  | Use Actual Valuation for Subassembly Receipt Posting. Allowed values: 1, 2 |
| `asrp_kw` | `string` | `varchar` |  |  | Use Actual Valuation for Subassembly Receipt Posting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `awtc` | `integer` | `int` |  |  | Use Actual Valuation for WIP Transfers. Allowed values: 1, 2 |
| `awtc_kw` | `string` | `varchar` |  |  | Use Actual Valuation for WIP Transfers (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `insp` | `integer` | `int` |  |  | Receipt Inspection Required. Allowed values: 1, 2 |
| `insp_kw` | `string` | `varchar` |  |  | Receipt Inspection Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fain` | `integer` | `int` |  |  | First Article Inspection. Allowed values: 0, 1, 2 |
| `fain_kw` | `string` | `varchar` |  |  | First Article Inspection (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `bmdl` | `string` | `varchar` |  |  | BOM Model. Max length: 9 |
| `bmrv` | `string` | `varchar` |  |  | BOM Revision. Max length: 6 |
| `rouc` | `string` | `varchar` |  |  | Routing Code. Max length: 9 |
| `rorv` | `string` | `varchar` |  |  | Routing Revision. Max length: 6 |
| `plgr` | `string` | `varchar` |  |  | Plan Group. Max length: 6 |
| `schd` | `integer` | `int` |  |  | Scheduled. Allowed values: 1, 2 |
| `schd_kw` | `string` | `varchar` |  |  | Scheduled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `irev` | `string` | `varchar` |  |  | Item Engineering Revision. Max length: 6 |
| `txta` | `integer` | `int` |  |  | Production Order Text |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tipcs030 Project Details |
| `mitm_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tiipd051 Items - Production by Site |
| `mitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tiipd001 Items - Production |
| `site_plgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tisch040 Work Center Plan Groups |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `rgrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs145 Routing Groups |
| `clco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `grid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tisfc350 Production Order Groups |
| `plid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `sfpl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `reco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `pldt_pltd` | `float` | `float` |  |  | CC: Planned Throughput Duration in Days |
| `adld_actd` | `float` | `float` |  |  | CC: Actual Throughput Duration in Days |
| `adld_dela` | `float` | `float` |  |  | CC: Delivered Late in Days |
| `adld_deea` | `float` | `float` |  |  | CC: Delivered Early in Days |
| `qbfd_buar` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Area |
| `qbfd_buln` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Length |
| `qbfd_bupc` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Piece |
| `qbfd_butm` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Time |
| `qbfd_buvl` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Volume |
| `qbfd_buwg` | `float` | `float` |  |  | CC: Quantity Backflushed in Base Unit Weight |
| `qdlv_buar` | `float` | `float` |  |  | CC: Quantity Delivered in Base Unit Area |
| `qdlv_buln` | `float` | `float` |  |  | CC: Quantity Delivered in Base Unit Length |
| `qdlv_bupc` | `float` | `float` |  |  | CC: Quantity Delivered in Base Unit Piece |
| `qdlv_butm` | `float` | `float` |  |  | CC: Quantity Delivered in Base Unit Time |
| `qdlv_buvl` | `float` | `float` |  |  | CC: Quantity Delivered in Base Unit Volume |
| `qdlv_buwg` | `float` | `float` |  |  | CC: Quantity Delivered in Base Unit Weight |
| `qntl_buar` | `float` | `float` |  |  | CC: Quantity Initial in Base Unit Area |
| `qntl_buln` | `float` | `float` |  |  | CC: Quantity Initial in Base Unit Length |
| `qntl_bupc` | `float` | `float` |  |  | CC: Quantity Initial in Base Unit Piece |
| `qntl_butm` | `float` | `float` |  |  | CC: Quantity Initial in Base Unit Time |
| `qntl_buvl` | `float` | `float` |  |  | CC: Quantity Initial in Base Unit Volume |
| `qntl_buwg` | `float` | `float` |  |  | CC: Quantity Initial in Base Unit Weight |
| `qrdr_buar` | `float` | `float` |  |  | CC: Quantity Ordered in Base Unit Area |
| `qrdr_buln` | `float` | `float` |  |  | CC: Quantity Ordered in Base Unit Length |
| `qrdr_bupc` | `float` | `float` |  |  | CC: Quantity Ordered in Base Unit Piece |
| `qrdr_butm` | `float` | `float` |  |  | CC: Quantity Ordered in Base Unit Time |
| `qrdr_buvl` | `float` | `float` |  |  | CC: Quantity Ordered in Base Unit Volume |
| `qrdr_buwg` | `float` | `float` |  |  | CC: Quantity Ordered in Base Unit Weight |
| `qqar_buar` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Area |
| `qqar_buln` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Length |
| `qqar_bupc` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Piece |
| `qqar_butm` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Time |
| `qqar_buvl` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Volume |
| `qqar_buwg` | `float` | `float` |  |  | CC: Quantity Quarantined in Base Unit Weight |
| `qtor_buar` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Area |
| `qtor_buln` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Length |
| `qtor_bupc` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Piece |
| `qtor_butm` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Time |
| `qtor_buvl` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Volume |
| `qtor_buwg` | `float` | `float` |  |  | CC: Quantity Rejected in Base Unit Weight |
| `qrjc_buar` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Area |
| `qrjc_buln` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Length |
| `qrjc_bupc` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Piece |
| `qrjc_butm` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Time |
| `qrjc_buvl` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Volume |
| `qrjc_buwg` | `float` | `float` |  |  | CC: Quantity Scrapped in Base Unit Weight |
| `qtbf_buar` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Area |
| `qtbf_buln` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Length |
| `qtbf_bupc` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Piece |
| `qtbf_butm` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Time |
| `qtbf_buvl` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Volume |
| `qtbf_buwg` | `float` | `float` |  |  | CC: Quantity to Backflush in Base Unit Weight |
| `qtdl_buar` | `float` | `float` |  |  | CC: Quantity to Deliver in Base Unit Area |
| `qtdl_buln` | `float` | `float` |  |  | CC: Quantity to Deliver in Base Unit Length |
| `qtdl_bupc` | `float` | `float` |  |  | CC: Quantity to Deliver in Base Unit Piece |
| `qtdl_butm` | `float` | `float` |  |  | CC: Quantity to Deliver in Base Unit Time |
| `qtdl_buvl` | `float` | `float` |  |  | CC: Quantity to Deliver in Base Unit Volume |
| `qtdl_buwg` | `float` | `float` |  |  | CC: Quantity to Deliver in Base Unit Weight |
| `mitm_opro_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Routing |
| `clco_fcmp` | `integer` | `int` |  |  | CC: Financial Company of Calculation Office |
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

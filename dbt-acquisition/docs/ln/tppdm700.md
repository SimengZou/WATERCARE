# tppdm700

LN tppdm700 Deliverables table, Generated 2026-04-10T19:42:03Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm700` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln`, `dlvr`, `schd`, `cprj`, `cspa`, `cpla`, `cact`, `sern` |
| **Column count** | 256 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
| `dlvr` | `integer` | `int` | `PK` | `not_null` | (required) Deliverable |
| `schd` | `integer` | `int` | `PK` | `not_null` | (required) Schedule |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `dlor` | `integer` | `int` |  |  | Origin. Allowed values: 10, 20 |
| `dlor_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tpctm.dlor.contract, tpctm.dlor.project |
| `milt` | `string` | `varchar` |  |  | Milestone. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `delt` | `integer` | `int` |  |  | Delivery Type. Allowed values: 1, 2, 3, 4, 5, 6 |
| `delt_kw` | `string` | `varchar` |  |  | Delivery Type (keyword). Allowed values: tppss.delt.wp, tppss.delt.pw, tppss.delt.ww, tppss.delt.wb, tppss.delt.pwb, tppss.delt.bw |
| `tdel` | `integer` | `int` |  |  | Type of Deliverable. Allowed values: 1, 2, 10 |
| `tdel_kw` | `string` | `varchar` |  |  | Type of Deliverable (keyword). Allowed values: tppdm.tdel.hardware, tppdm.tdel.nonhardware, tppdm.tdel.maintenance |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `cpva` | `integer` | `int` |  |  | Product Variant |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oqan` | `float` | `float` |  |  | Ordered Quantity |
| `ouni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `cqan` | `float` | `float` |  |  | Completed Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `sqan` | `float` | `float` |  |  | Delivered Quantity |
| `suni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `rqan` | `float` | `float` |  |  | Returned Quantity |
| `aqan` | `float` | `float` |  |  | Accepted Quantity |
| `bqan` | `float` | `float` |  |  | Backorder Quantity |
| `buni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `baor` | `integer` | `int` |  |  | Backorder. Allowed values: 1, 2 |
| `baor_kw` | `string` | `varchar` |  |  | Backorder (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `osch` | `integer` | `int` |  |  | Backorder Original Schedule |
| `rddt` | `timestamp` | `timestamp_ntz` |  |  | Contract Delivery Date |
| `cddt` | `timestamp` | `timestamp_ntz` |  |  | Confirmed Delivery Date |
| `iddt` | `timestamp` | `timestamp_ntz` |  |  | Internal Delivery Date |
| `pddt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `mrlc` | `string` | `varchar` |  |  | Maintenance Receipt Location. Max length: 6 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `cocu` | `string` | `varchar` |  |  | Cost Currency. Max length: 3 |
| `rtcc_1` | `float` | `float` |  |  | Currency Rate (Cost) |
| `rtcc_2` | `float` | `float` |  |  | Currency Rate (Cost) |
| `rtcc_3` | `float` | `float` |  |  | Currency Rate (Cost) |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor Costs |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor Costs |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor Costs |
| `pris` | `float` | `float` |  |  | Sales Price |
| `sacu` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rtcs_1` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtcs_2` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtcs_3` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtfs_1` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_2` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_3` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `acpn` | `integer` | `int` |  |  | Acceptance Point. Allowed values: 10, 20, 30, 40 |
| `acpn_kw` | `string` | `varchar` |  |  | Acceptance Point (keyword). Allowed values: tpctm.acpn.source, tpctm.acpn.destination, tpctm.acpn.source.dest, tpctm.acpn.not.appl |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30, 35, 40, 50 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tpctm.dlst.free, tpctm.dlst.active, tpctm.dlst.released.to.wh, tpctm.dlst.delivered, tpctm.dlst.closed, tpctm.dlst.canceled |
| `loco` | `string` | `varchar` |  |  | User. Max length: 16 |
| `twar` | `string` | `varchar` |  |  | To Warehouse. Max length: 6 |
| `tprj` | `string` | `varchar` |  |  | To Project. Max length: 9 |
| `tpla` | `string` | `varchar` |  |  | To Plan. Max length: 3 |
| `tspa` | `string` | `varchar` |  |  | To Element. Max length: 16 |
| `tact` | `string` | `varchar` |  |  | To Activity. Max length: 16 |
| `tstl` | `string` | `varchar` |  |  | To Extension. Max length: 4 |
| `tcco` | `string` | `varchar` |  |  | To Cost Component. Max length: 8 |
| `carr` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `serv` | `string` | `varchar` |  |  | Freight Service Level. Max length: 3 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `camt` | `float` | `float` |  |  | Cost Amount |
| `samt` | `float` | `float` |  |  | Sales Amount |
| `amts` | `float` | `float` |  |  | Settlement Amount |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `citt` | `string` | `varchar` |  |  | Item Coding System. Max length: 3 |
| `crbn` | `integer` | `int` |  |  | Carrier / LSP Binding. Allowed values: 0, 1, 2 |
| `crbn_kw` | `string` | `varchar` |  |  | Carrier / LSP Binding (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 0, 2, 4, 6, 8, 10, 12, 13, 14, 16, 18, 20, 30, 50, 200, 205, 210, 215 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: , tpgen.porg.not.applicable, tpgen.porg.manual, tpgen.porg.contract, tpgen.porg.variant, tpgen.porg.item.pur, tpgen.porg.item.sls, tpgen.porg.item.service, tpgen.porg.supp.pr.book, tpgen.porg.def.pr.book, tpgen.porg.price.structure, tpgen.porg.extern, tpgen.porg.generic.pr.list, tpgen.porg.budget, tpgen.porg.equipment.proj, tpgen.porg.equipment.stand, tpgen.porg.subcontr.proj, tpgen.porg.subcontr.stand |
| `orno` | `string` | `varchar` |  |  | Warehouse Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Line |
| `seqn` | `integer` | `int` |  |  | Sequence |
| `ltsl` | `integer` | `int` |  |  | Lot Selection. Allowed values: 10, 20, 30, 40 |
| `ltsl_kw` | `string` | `varchar` |  |  | Lot Selection (keyword). Allowed values: tpctm.ltsl.any, tpctm.ltsl.same, tpctm.ltsl.specific, tpctm.ltsl.not.applicable |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `srsl` | `integer` | `int` |  |  | Serial Selection. Allowed values: 10, 20, 30 |
| `srsl_kw` | `string` | `varchar` |  |  | Serial Selection (keyword). Allowed values: tpctm.srsl.any, tpctm.srsl.specific, tpctm.srsl.not.applicable |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `lssn` | `string` | `varchar` |  |  | Item Identification Set. Max length: 22 |
| `dlpt` | `string` | `varchar` |  |  | Delivery Point. Max length: 9 |
| `shcn` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 10, 20, 30 |
| `shcn_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tpctm.shcn.none, tpctm.shcn.ship.line.compl, tpctm.shcn.ship.line.cancl |
| `dspr` | `integer` | `int` |  |  | Delivery Schedule Present. Allowed values: 1, 2 |
| `dspr_kw` | `string` | `varchar` |  |  | Delivery Schedule Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `unbd` | `integer` | `int` |  |  | Unit Binding. Allowed values: 1, 2 |
| `unbd_kw` | `string` | `varchar` |  |  | Unit Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cspg` | `integer` | `int` |  |  | Cost Pegged. Allowed values: 1, 2 |
| `cspg_kw` | `string` | `varchar` |  |  | Cost Pegged (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crby` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modified on |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `cnpr` | `string` | `varchar` |  |  | Contract Project. Max length: 9 |
| `cnel` | `string` | `varchar` |  |  | Contract Element. Max length: 16 |
| `cnpl` | `string` | `varchar` |  |  | Contract Plan. Max length: 3 |
| `cnac` | `string` | `varchar` |  |  | Contract Activity. Max length: 16 |
| `inst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `rfcu__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 40 |
| `rlcu__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order Line - base language. Max length: 16 |
| `rtor` | `integer` | `int` |  |  | Return Deliverable. Allowed values: 1, 2 |
| `rtor_kw` | `string` | `varchar` |  |  | Return Deliverable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rfsh` | `string` | `varchar` |  |  | Return From Shipment. Max length: 9 |
| `rfic` | `integer` | `int` |  |  | Return From Invoice Company |
| `rfit` | `string` | `varchar` |  |  | Return From Invoice Transaction Type. Max length: 3 |
| `rfid` | `integer` | `int` |  |  | Return From Invoice Document |
| `rfdl` | `integer` | `int` |  |  | Return From Deliverable |
| `rfsc` | `integer` | `int` |  |  | Return From Schedule |
| `rrsn` | `string` | `varchar` |  |  | Return Reason. Max length: 6 |
| `rown` | `integer` | `int` |  |  | Return Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `rown_kw` | `string` | `varchar` |  |  | Return Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `ccfm` | `integer` | `int` |  |  | Contains Customer Furnished Material. Allowed values: 1, 2 |
| `ccfm_kw` | `string` | `varchar` |  |  | Contains Customer Furnished Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sign` | `integer` | `int` |  |  | Planning Exception. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20, 22, 25, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 40, 45, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 250 |
| `sign_kw` | `string` | `varchar` |  |  | Planning Exception (keyword). Allowed values: tprao.sign.inven.lo.zero, tprao.sign.inven.lo.safety, tprao.sign.inven.hi.max, tprao.sign.actual.to.late, tprao.sign.demf.dif.act, tprao.sign.astc.hi.pstc, tprao.sign.sfc.lo.zero, tprao.sign.dem.is.zero, tprao.sign.dem.orders.zero, tprao.sign.orqa.lo.min, tprao.sign.orqa.hi.max, tprao.sign.orqa.no.mult, tprao.sign.orqa.no.fix, tprao.sign.item.sup, tprao.sign.project, tprao.sign.no.inbound, tprao.sign.no.outbound, tprao.sign.release.to.late, tprao.sign.cancel, tprao.sign.reschedule.in, tprao.sign.reschedule.out, tprao.sign.order.in.safety, tprao.sign.order.in.inter, tprao.sign.order.in.eltm, tprao.sign.order.late, tprao.sign.over.util, tprao.sign.work.lo.norm, tprao.sign.work.hi.norm, tprao.sign.atp.lo.zero, tprao.sign.transfer.fail, tprao.sign.supply.demand, tprao.sign.fatal, tprao.sign.rpt.error, tprao.sign.no.supply, tprao.sign.no.bpid, tprao.sign.specification, tprao.sign.item, tprao.sign.empty.plan.unit, tprao.sign.wlc.parameters, tprao.sign.wlc.cap.constr, tprao.sign.wlc.mat.constr, tprao.sign.plan.failure, tprao.sign.peg.no.stock, tprao.sign.peg.pot.stock.s, tprao.sign.peg.proj.stock, tprao.sign.peg.sup.in.past, tprao.sign.peg.sup.late, tprao.sign.peg.sup.early, tprao.sign.peg.m.no.stock, tprao.sign.peg.m.pot.stck, tprao.sign.peg.m.proj.stck, tprao.sign.peg.m.sup.in.pa, tprao.sign.peg.m.sup.late, tprao.sign.peg.m.sup.early, tprao.sign.comm.quan.late, tprao.sign.comm.quan.early, tprao.sign.comm.quan.hi, tprao.sign.comm.quan.lo, tprao.sign.peg.sup.oversup, tprao.sign.peg.sup.over.fc, tprao.sign.peg.excluded, tprao.sign.no.exception |
| `prsg` | `string` | `varchar` |  |  | Price Stage. Max length: 3 |
| `tcst` | `integer` | `int` |  |  | Document Compliance Status. Allowed values: 10, 20, 30, 40, 50 |
| `tcst_kw` | `string` | `varchar` |  |  | Document Compliance Status (keyword). Allowed values: tcgtc.tcst.to.be.validated, tcgtc.tcst.validating, tcgtc.tcst.valid.error, tcgtc.tcst.validated, tcgtc.tcst.not.appl |
| `tcff` | `integer` | `int` |  |  | Compliance Failure for. Allowed values: 10, 20, 30, 40, 50, 60, 70 |
| `tcff_kw` | `string` | `varchar` |  |  | Compliance Failure for (keyword). Allowed values: tpgtc.fail.src.not.applicable, tpgtc.fail.src.trade.compl, tpgtc.fail.src.embargo, tpgtc.fail.src.boycott, tpgtc.fail.src.letter.of.cred, tpgtc.fail.src.bank.guar.bene, tpgtc.fail.src.bank.guar.appl |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `pass` | `integer` | `int` |  |  | Process After Sales Service Automatically. Allowed values: 1, 2 |
| `pass_kw` | `string` | `varchar` |  |  | Process After Sales Service Automatically (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tatd` | `string` | `varchar` |  |  | Turnaround Time Document. Max length: 9 |
| `lcrq` | `integer` | `int` |  |  | Letter of Credit Required. Allowed values: 1, 2 |
| `lcrq_kw` | `string` | `varchar` |  |  | Letter of Credit Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `disc` | `float` | `float` |  |  | Discount Percentage |
| `ldam` | `float` | `float` |  |  | Discount Amount |
| `dorg` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12, 200, 205, 210, 215 |
| `dorg_kw` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tpgen.dorg.not.applicable, tpgen.dorg.manual, tpgen.dorg.contract, tpgen.dorg.disc.structure, tpgen.dorg.disc.pr.book, tpgen.dorg.extern, tpgen.dorg.equipment.proj, tpgen.dorg.equipment.stand, tpgen.dorg.subcontr.proj, tpgen.dorg.subcontr.stand |
| `bgrq` | `integer` | `int` |  |  | Bank Guarantee - Applicant Required. Allowed values: 1, 2 |
| `bgrq_kw` | `string` | `varchar` |  |  | Bank Guarantee - Applicant Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrb` | `integer` | `int` |  |  | Bank Guarantee - Beneficiary Required. Allowed values: 1, 2 |
| `bgrb_kw` | `string` | `varchar` |  |  | Bank Guarantee - Beneficiary Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `blck` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `blck_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_cpla_milt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `item_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `ouni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `suni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `buni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_dlpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom134 Delivery Points |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cocu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `sacu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `twar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `tprj_tstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `tprj_tpla_tact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `tprj_tspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `tprj_tpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `tprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `tcco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `carr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `serv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs075 Freight Service Levels |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `lssn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd410 Item Identification Sets |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cnpr_cnel_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cnpr_cnpl_cnac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cnpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `rrsn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `prsg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs212 Price Stages |
| `tatd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcttm100 Turnaround Time Documents |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `samt_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `samt_cntc` | `float` | `float` |  |  | CC: Sales Amount in Contract Currency |
| `samt_prjc` | `float` | `float` |  |  | CC: Sales Amount in Project Currency |
| `samt_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `samt_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `samt_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `samt_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
| `camt_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `camt_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `camt_prjc` | `float` | `float` |  |  | CC: Cost Amount in Project Currency |
| `camt_homc` | `float` | `float` |  |  | CC: Cost Amount in Local Currency |
| `camt_rpc1` | `float` | `float` |  |  | CC: Cost Amount in Reporting Currency 1 |
| `camt_rpc2` | `float` | `float` |  |  | CC: Cost Amount in Reporting Currency 2 |
| `camt_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
| `amts_refc` | `float` | `float` |  |  | CC: Settlement Amount in Reference Currency |
| `amts_cntc` | `float` | `float` |  |  | CC: Settlement Amount in Contract Currency |
| `amts_prjc` | `float` | `float` |  |  | CC: Settlement Amount in Project Currency |
| `amts_homc` | `float` | `float` |  |  | CC: Settlement Amount in Local Currency |
| `amts_rpc1` | `float` | `float` |  |  | CC: Settlement Amount in Reporting Currency 1 |
| `amts_rpc2` | `float` | `float` |  |  | CC: Settlement Amount in Reporting Currency 2 |
| `amts_dwhc` | `float` | `float` |  |  | CC: Settlement Amount in Data Warehouse Currency |
| `cstv_refc` | `float` | `float` |  |  | CC: Customs Value in Reference Currency |
| `cstv_cntc` | `float` | `float` |  |  | CC: Customs Value in Contract Currency |
| `cstv_prjc` | `float` | `float` |  |  | CC: Customs Value in Project Currency |
| `cstv_homc` | `float` | `float` |  |  | CC: Customs Value in Local Currency |
| `cstv_rpc1` | `float` | `float` |  |  | CC: Customs Value in Reporting Currency 1 |
| `cstv_rpc2` | `float` | `float` |  |  | CC: Customs Value in Reporting Currency 2 |
| `cstv_dwhc` | `float` | `float` |  |  | CC: Customs Value in Data Warehouse Currency |
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

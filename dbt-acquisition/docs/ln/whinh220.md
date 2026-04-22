# whinh220

LN whinh220 Outbound Order Lines table, Generated 2026-04-10T19:42:48Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh220` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `oorg`, `orno`, `pono`, `seqn` |
| **Column count** | 216 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `oorg` | `integer` | `int` | `PK` | `not_null` | (required) Order Origin. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `oorg_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `oset` | `integer` | `int` |  |  | Set |
| `comp` | `integer` | `int` |  |  | Ship-from Company |
| `cwar` | `string` | `varchar` |  |  | Shipping Warehouse. Max length: 6 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `dlpt` | `string` | `varchar` |  |  | Delivery Point. Max length: 9 |
| `acvt` | `integer` | `int` |  |  | Activated. Allowed values: 1, 2 |
| `acvt_kw` | `string` | `varchar` |  |  | Activated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `dtag` | `string` | `varchar` |  |  | Demand Tag. Max length: 9 |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `ssts` | `integer` | `int` |  |  | Serial Status to be Picked. Allowed values: 1, 2, 3, 4, 5, 6, 7, 20 |
| `ssts_kw` | `string` | `varchar` |  |  | Serial Status to be Picked (keyword). Allowed values: whibd.ssts.initial, whibd.ssts.used.in.as.buil, whibd.ssts.used.in.as.main, whibd.ssts.defective, whibd.ssts.working.cond, whibd.ssts.to.be.recycled, whibd.ssts.removed, whibd.ssts.not.appl |
| `lsel` | `integer` | `int` |  |  | Lot Selection. Allowed values: 1, 2, 3 |
| `lsel_kw` | `string` | `varchar` |  |  | Lot Selection (keyword). Allowed values: tclsel.any, tclsel.same, tclsel.specific |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `revi` | `string` | `varchar` |  |  | E-item Revision. Max length: 6 |
| `circ__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item Revision - base language. Max length: 35 |
| `prio` | `integer` | `int` |  |  | Order Priority |
| `qoro` | `float` | `float` |  |  | Ordered Quantity in Order Unit |
| `orun` | `string` | `varchar` |  |  | Order Unit. Max length: 3 |
| `ubin` | `integer` | `int` |  |  | Unit Binding. Allowed values: 1, 2 |
| `ubin_kw` | `string` | `varchar` |  |  | Unit Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hstq` | `integer` | `int` |  |  | Hard Stop on Quantity. Allowed values: 1, 2, 3 |
| `hstq_kw` | `string` | `varchar` |  |  | Hard Stop on Quantity (keyword). Allowed values: tchstp.no, tchstp.warn, tchstp.block |
| `qord` | `float` | `float` |  |  | Ordered Quantity |
| `qorp` | `float` | `float` |  |  | Ordered Quantity in Piece Unit |
| `qlin` | `float` | `float` |  |  | Linked Quantity in Inventory Unit |
| `qlip` | `float` | `float` |  |  | Linked Quantity in Piece Unit |
| `qadv` | `float` | `float` |  |  | Advised Quantity in Inventory Unit |
| `qadp` | `float` | `float` |  |  | Advised Quantity in Piece Unit |
| `qrel` | `float` | `float` |  |  | Released Quantity in Inventory Unit |
| `qrep` | `float` | `float` |  |  | Released Quantity in Piece Unit |
| `qpic` | `float` | `float` |  |  | Picked Quantity |
| `qpip` | `float` | `float` |  |  | Picked Quantity in Piece Unit |
| `qapr` | `float` | `float` |  |  | Approved Quantity |
| `qapp` | `float` | `float` |  |  | Approved Quantity in Piece Unit |
| `qrej` | `float` | `float` |  |  | Rejected Quantity |
| `qrpu` | `float` | `float` |  |  | Rejected Quantity in Piece Unit |
| `qrsc` | `float` | `float` |  |  | Scrapped Quantity |
| `qrsp` | `float` | `float` |  |  | Scrapped Quantity in Piece Unit |
| `qnse` | `float` | `float` |  |  | Expected Not Shipped Quantity |
| `qnsp` | `float` | `float` |  |  | Expected Not Shipped Quantity in Piece Unit |
| `qnsh` | `float` | `float` |  |  | Not Shipped Quantity in Inventory Unit |
| `qnpu` | `float` | `float` |  |  | Not Shipped Quantity in Piece Unit |
| `qshp` | `float` | `float` |  |  | Shipped Quantity in Inventory Unit |
| `qspu` | `float` | `float` |  |  | Shipped Quantity in Piece Unit |
| `qoor` | `float` | `float` |  |  | Originally Ordered Quantity |
| `qoop` | `float` | `float` |  |  | Originally Ordered Quantity in Piece Unit |
| `qova` | `float` | `float` |  |  | Allowed Overdelivery Quantity |
| `qovp` | `float` | `float` |  |  | Allowed Overdelivery Quantity in Piece Unit |
| `qovd` | `float` | `float` |  |  | Overdelivery |
| `qopu` | `float` | `float` |  |  | Overdelivery in Piece Unit |
| `oarq` | `integer` | `int` |  |  | Outbound Advice Required. Allowed values: 1, 2 |
| `oarq_kw` | `string` | `varchar` |  |  | Outbound Advice Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scon` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `scon_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tcscon.none, tcscon.sc, tcscon.rc, tcscon.oc, tcscon.sca, tcscon.kc, tcscon.not.applicable |
| `qcnl` | `float` | `float` |  |  | Canceled Quantity |
| `qcnp` | `float` | `float` |  |  | Canceled Quantity in Piece Unit |
| `qscl` | `float` | `float` |  |  | Shipment Canceled Quantity |
| `qscp` | `float` | `float` |  |  | Shipment Canceled Quantity in Pieces |
| `fisc` | `integer` | `int` |  |  | Finalize Order Line after Shipment Cancelation. Allowed values: 1, 2 |
| `fisc_kw` | `string` | `varchar` |  |  | Finalize Order Line after Shipment Cancelation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pddt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `addt` | `timestamp` | `timestamp_ntz` |  |  | Actual Delivery Date |
| `inup` | `integer` | `int` |  |  | Inventory Handling. Allowed values: 1, 2 |
| `inup_kw` | `string` | `varchar` |  |  | Inventory Handling (keyword). Allowed values: tcinup.by.main.item, tcinup.by.component |
| `bflh` | `integer` | `int` |  |  | Backflush Order. Allowed values: 1, 2 |
| `bflh_kw` | `string` | `varchar` |  |  | Backflush Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rush` | `integer` | `int` |  |  | Rush Order. Allowed values: 1, 2 |
| `rush_kw` | `string` | `varchar` |  |  | Rush Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdck` | `integer` | `int` |  |  | Cross-docking. Allowed values: 1, 2 |
| `cdck_kw` | `string` | `varchar` |  |  | Cross-docking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qreq` | `float` | `float` |  |  | Requested Quantity to Cross-dock |
| `rqpu` | `float` | `float` |  |  | Requested Quantity to Cross-dock in Piece Unit |
| `qcad` | `float` | `float` |  |  | Advised Quantity to Cross-dock |
| `qcap` | `float` | `float` |  |  | Advised Quantity to Cross-dock in Piece Unit |
| `qact` | `float` | `float` |  |  | Cross-docked Quantity |
| `qacp` | `float` | `float` |  |  | Cross-docked Quantity in Piece Unit |
| `qpss` | `float` | `float` |  |  | Planned Shipment Quantity in Order Unit |
| `qpsv` | `float` | `float` |  |  | Planned Shipment Quantity in Inventory Unit |
| `qpsp` | `float` | `float` |  |  | Planned Shipment Quantity in Piece Unit |
| `qprv` | `float` | `float` |  |  | Projected Quantity in Inventory Unit |
| `qprp` | `float` | `float` |  |  | Projected Quantity in Piece Unit |
| `masp` | `integer` | `int` |  |  | Manual Shipment Planning. Allowed values: 1, 2 |
| `masp_kw` | `string` | `varchar` |  |  | Manual Shipment Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shrt` | `integer` | `int` |  |  | Shortage. Allowed values: 1, 2 |
| `shrt_kw` | `string` | `varchar` |  |  | Shortage (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `blck` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `blck_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cncl` | `integer` | `int` |  |  | Canceled. Allowed values: 1, 2 |
| `cncl_kw` | `string` | `varchar` |  |  | Canceled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bcko` | `integer` | `int` |  |  | Create Backorders. Allowed values: 1, 2 |
| `bcko_kw` | `string` | `varchar` |  |  | Create Backorders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pkdf` | `string` | `varchar` |  |  | Package Definition. Max length: 6 |
| `pkdb` | `integer` | `int` |  |  | Package Definition Binding. Allowed values: 1, 2 |
| `pkdb_kw` | `string` | `varchar` |  |  | Package Definition Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iubw` | `integer` | `int` |  |  | In Use by WMS. Allowed values: 1, 2 |
| `iubw_kw` | `string` | `varchar` |  |  | In Use by WMS (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `gefo` | `integer` | `int` |  |  | Generate Freight Order from Warehousing. Allowed values: 1, 2 |
| `gefo_kw` | `string` | `varchar` |  |  | Generate Freight Order from Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fmor` | `string` | `varchar` |  |  | Freight Order. Max length: 9 |
| `fmln` | `integer` | `int` |  |  | Freight Order Line |
| `ovlp` | `integer` | `int` |  |  | Overrule Load Plan. Allowed values: 1, 2 |
| `ovlp_kw` | `string` | `varchar` |  |  | Overrule Load Plan (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ovfp` | `integer` | `int` |  |  | Overrule Fulfillment Plan. Allowed values: 1, 2 |
| `ovfp_kw` | `string` | `varchar` |  |  | Overrule Fulfillment Plan (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uwop` | `integer` | `int` |  |  | Use Warehouse Order Price. Allowed values: 1, 2 |
| `uwop_kw` | `string` | `varchar` |  |  | Use Warehouse Order Price (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `orpr` | `float` | `float` |  |  | Order Price |
| `ocur` | `string` | `varchar` |  |  | Order Price Currency. Max length: 3 |
| `csvl` | `float` | `float` |  |  | Customs Value |
| `csvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `rorg` | `integer` | `int` |  |  | Originating Order Origin for Return. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `rorg_kw` | `string` | `varchar` |  |  | Originating Order Origin for Return (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `rorn` | `string` | `varchar` |  |  | Originating Order for Return. Max length: 9 |
| `rpon` | `integer` | `int` |  |  | Originating Order Line for Return |
| `rseq` | `integer` | `int` |  |  | Originating Order Sequence for Return |
| `paym` | `integer` | `int` |  |  | Payment. Allowed values: 10, 20, 30, 90 |
| `paym_kw` | `string` | `varchar` |  |  | Payment (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `ipay` | `integer` | `int` |  |  | Internal Payment. Allowed values: 10, 20, 30, 90 |
| `ipay_kw` | `string` | `varchar` |  |  | Internal Payment (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `istr` | `integer` | `int` |  |  | Issue Strategy. Allowed values: 10, 20, 30, 90 |
| `istr_kw` | `string` | `varchar` |  |  | Issue Strategy (keyword). Allowed values: tcistr.free, tcistr.preferred, tcistr.restricted, tcistr.not.appl |
| `ifbp` | `string` | `varchar` |  |  | Issue from Business Partner. Max length: 9 |
| `iown` | `integer` | `int` |  |  | Issue Ownership. Allowed values: 10, 20, 30, 40, 90 |
| `iown_kw` | `string` | `varchar` |  |  | Issue Ownership (keyword). Allowed values: tciown.default, tciown.comp.owned, tciown.consigned, tciown.cust.owned, tciown.not.appl |
| `huwt` | `integer` | `int` |  |  | Usage at Warehouse Transfer. Allowed values: 10, 20, 30, 90 |
| `huwt_kw` | `string` | `varchar` |  |  | Usage at Warehouse Transfer (keyword). Allowed values: tchuwt.yes, tchuwt.no, tchuwt.terms.and.cond, tchuwt.not.appl |
| `fprj` | `string` | `varchar` |  |  | From Project. Max length: 9 |
| `fspa` | `string` | `varchar` |  |  | From Element. Max length: 16 |
| `fact` | `string` | `varchar` |  |  | From Activity. Max length: 16 |
| `fstl` | `string` | `varchar` |  |  | From Extension. Max length: 4 |
| `fcco` | `string` | `varchar` |  |  | From Cost Component. Max length: 8 |
| `tprj` | `string` | `varchar` |  |  | To Project. Max length: 9 |
| `tspa` | `string` | `varchar` |  |  | To Element. Max length: 16 |
| `tact` | `string` | `varchar` |  |  | To Activity. Max length: 16 |
| `tstl` | `string` | `varchar` |  |  | To Extension. Max length: 4 |
| `tcco` | `string` | `varchar` |  |  | To Cost Component. Max length: 8 |
| `prjp` | `integer` | `int` |  |  | Peg Distribution. Allowed values: 1, 2 |
| `prjp_kw` | `string` | `varchar` |  |  | Peg Distribution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acpn` | `integer` | `int` |  |  | Acceptance Point. Allowed values: 10, 20, 30, 40 |
| `acpn_kw` | `string` | `varchar` |  |  | Acceptance Point (keyword). Allowed values: whinh.acpn.source, whinh.acpn.destination, whinh.acpn.source.dest, whinh.acpn.not.appl |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 30 |
| `refs__en_us` | `string` | `varchar` |  | `not_null` | (required) Shipment Reference - base language. Max length: 35 |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 40 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `casi` | `string` | `varchar` |  |  | Extra Intrastat Info. Max length: 3 |
| `gnld` | `string` | `varchar` |  |  | General Ledger. Max length: 22 |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `iscn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `iscn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imcs` | `string` | `varchar` |  |  | Intermediate Consignee. Max length: 35 |
| `sups` | `integer` | `int` |  |  | Supply System. Allowed values: 10, 20, 30, 40, 50, 60, 90, 100 |
| `sups_kw` | `string` | `varchar` |  |  | Supply System (keyword). Allowed values: whsups.tpop, whsups.kanban, whsups.batch, whsups.sils, whsups.single, whsups.sic, whsups.manual, whsups.other |
| `wmss` | `integer` | `int` |  |  | WMS Status. Allowed values: 10, 30, 40, 50, 100 |
| `wmss_kw` | `string` | `varchar` |  |  | WMS Status (keyword). Allowed values: tcwmss.expected, tcwmss.sent, tcwmss.in.process, tcwmss.closed, tcwmss.not.appl |
| `lsta` | `integer` | `int` |  |  | Line Status. Allowed values: 2, 3, 5, 10, 15, 20, 22, 23, 25, 30 |
| `lsta_kw` | `string` | `varchar` |  |  | Line Status (keyword). Allowed values: whinh.lstb.planned, whinh.lstb.tag.linked, whinh.lstb.open, whinh.lstb.adviced, whinh.lstb.released, whinh.lstb.picked, whinh.lstb.to.be.inspected, whinh.lstb.inspected, whinh.lstb.staged, whinh.lstb.shipped |
| `exsp` | `integer` | `int` |  |  | Expedited Shipment Lines Present. Allowed values: 1, 2 |
| `exsp_kw` | `string` | `varchar` |  |  | Expedited Shipment Lines Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txtn` | `integer` | `int` |  |  | Outbound Text |
| `oorg_orno_oset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh200 Warehousing Orders |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `stad_dlpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom134 Delivery Points |
| `item_clot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whltc100 Lots |
| `item_atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd466 Item Warehousing - Attribute Sets |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `orun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `pkdf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd410 Package Definitions |
| `ocur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `csvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `fprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `fcco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `tprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `tcco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `txtn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `conv_buar` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Area |
| `conv_buln` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Length |
| `conv_buvl` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Volume |
| `conv_buwg` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Weight |
| `amnt_lclc` | `float` | `float` |  |  | CC: Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Amount in Reporting Currency 2 |
| `amnt_rfrc` | `float` | `float` |  |  | CC: Amount in Reference Currency |
| `amnt_dtwc` | `float` | `float` |  |  | CC: Amount in Data Warehouse Currency |
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

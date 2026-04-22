# tssoc200

LN tssoc200 Service Orders table, Generated 2026-04-10T19:42:37Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tssoc200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno` |
| **Column count** | 261 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Service Order. Max length: 9 |
| `clst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `sern` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `lcad` | `string` | `varchar` |  |  | Location Address. Max length: 9 |
| `ppeg` | `integer` | `int` |  |  | Project Pegged. Allowed values: 1, 2 |
| `ppeg_kw` | `string` | `varchar` |  |  | Project Pegged (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `pcac` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `inby` | `integer` | `int` |  |  | Invoicing By. Allowed values: 5, 10, 99 |
| `inby_kw` | `string` | `varchar` |  |  | Invoicing By (keyword). Allowed values: tcinby.project, tcinby.service, tcinby.not.appl |
| `cpor` | `integer` | `int` |  |  | Project Peg Origin. Allowed values: 5, 10, 15, 100 |
| `cpor_kw` | `string` | `varchar` |  |  | Project Peg Origin (keyword). Allowed values: tcpeg.cpor.top.demand, tcpeg.cpor.costing.break, tcpeg.cpor.manual, tcpeg.cpor.not.applicable |
| `ordt` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `osta` | `integer` | `int` |  |  | Order Status. Allowed values: 5, 10, 15, 20, 23, 25, 35, 40 |
| `osta_kw` | `string` | `varchar` |  |  | Order Status (keyword). Allowed values: tssoc.osta.free, tssoc.osta.planned, tssoc.osta.released, tssoc.osta.completed, tssoc.osta.costed, tssoc.osta.closed, tssoc.osta.canceled, tssoc.osta.rejected |
| `fdpt` | `string` | `varchar` |  |  | Financial Department. Max length: 6 |
| `cwoc` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `csar` | `string` | `varchar` |  |  | Service Area. Max length: 3 |
| `emno` | `string` | `varchar` |  |  | Preferred Engineer. Max length: 9 |
| `ccar` | `string` | `varchar` |  |  | Service Car. Max length: 10 |
| `blyn` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `blyn_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ityn` | `integer` | `int` |  |  | Interrupted. Allowed values: 1, 2 |
| `ityn_kw` | `string` | `varchar` |  |  | Interrupted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crit` | `string` | `varchar` |  |  | Interruption Reason. Max length: 6 |
| `ordu` | `float` | `float` |  |  | Order Duration |
| `tvdu` | `float` | `float` |  |  | Travel Duration |
| `trdi` | `float` | `float` |  |  | Travel Distance |
| `pstm` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Time |
| `pftm` | `timestamp` | `timestamp_ntz` |  |  | Planned Finish Time |
| `ptft` | `timestamp` | `timestamp_ntz` |  |  | Planned Travel Finish Time |
| `ptst` | `timestamp` | `timestamp_ntz` |  |  | Planned Travel Start Time |
| `estm` | `timestamp` | `timestamp_ntz` |  |  | Earliest Start Time |
| `lftm` | `timestamp` | `timestamp_ntz` |  |  | Latest Finish Time |
| `otim` | `integer` | `int` |  |  | Overtime. Allowed values: 1, 2 |
| `otim_kw` | `string` | `varchar` |  |  | Overtime (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `appo` | `integer` | `int` |  |  | Appointment. Allowed values: 1, 2 |
| `appo_kw` | `string` | `varchar` |  |  | Appointment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tstm` | `timestamp` | `timestamp_ntz` |  |  | Actual Travel Start Time |
| `astm` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Time |
| `aftm` | `timestamp` | `timestamp_ntz` |  |  | Actual Finish Time |
| `cosh` | `string` | `varchar` |  |  | Service Order Costing Sheet. Max length: 9 |
| `crep` | `string` | `varchar` |  |  | Sales Representative (Inside). Max length: 9 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Area. Max length: 3 |
| `cplt` | `string` | `varchar` |  |  | Sales Price List. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rats_1` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_2` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_3` | `float` | `float` |  |  | Currency Rate Sales |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rtdt` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Rate Type. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `csco` | `string` | `varchar` |  |  | Service Contract. Max length: 9 |
| `pmtd` | `integer` | `int` |  |  | Pricing Method. Allowed values: 5, 10, 12, 15, 25, 30 |
| `pmtd_kw` | `string` | `varchar` |  |  | Pricing Method (keyword). Allowed values: tsmdm.pmtd.not.applicable, tsmdm.pmtd.fixed.act.pr, tsmdm.pmtd.fixed.order.pr, tsmdm.pmtd.time.material, tsmdm.pmtd.quo.fixed, tsmdm.pmtd.quo.fixedplus |
| `pcon` | `string` | `varchar` |  |  | Pricing Contract. Max length: 9 |
| `pcch` | `integer` | `int` |  |  | Pricing Contract Change |
| `pcln` | `integer` | `int` |  |  | Pricing Contract Line |
| `csqu` | `string` | `varchar` |  |  | Service Order Quote. Max length: 9 |
| `espr` | `float` | `float` |  |  | Estimated Sales Price |
| `pris` | `float` | `float` |  |  | Sales Price |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 5, 10, 15, 20, 25, 30, 35, 45, 50, 55, 60, 99 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: tstdm.porg.manual, tstdm.porg.other, tstdm.porg.serv.contract, tstdm.porg.master.routing, tstdm.porg.routing.option, tstdm.porg.ref.activity, tstdm.porg.trav.rate.book, tstdm.porg.item.serv, tstdm.porg.def.pr.book, tstdm.porg.price.structure, tstdm.porg.serial, tstdm.porg.not.applicable |
| `bppr` | `string` | `varchar` |  |  | Business Partner Prices and Discounts. Max length: 9 |
| `pldd` | `string` | `varchar` |  |  | Price List for Direct Delivery. Max length: 3 |
| `odis` | `float` | `float` |  |  | Order Discount |
| `oamt` | `float` | `float` |  |  | Estimated Order Amount |
| `amnt` | `float` | `float` |  |  | Actual Order Amount |
| `tinv` | `integer` | `int` |  |  | Total Invoice. Allowed values: 1, 2 |
| `tinv_kw` | `string` | `varchar` |  |  | Total Invoice (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `camr` | `string` | `varchar` |  |  | Master Routing. Max length: 16 |
| `caro` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Sold-to Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Sold-to Contact. Max length: 9 |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 40 |
| `corp__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order Position - base language. Max length: 16 |
| `cors__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order Sequence - base language. Max length: 11 |
| `refa__en_us` | `string` | `varchar` |  | `not_null` | (required) First Reference - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Second Reference - base language. Max length: 30 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `itcn` | `string` | `varchar` |  |  | Invoice-to Contact. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `pfcn` | `string` | `varchar` |  |  | Pay-by Contact. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `smto` | `integer` | `int` |  |  | Ship Materials To. Allowed values: 10, 20, 30, 40, 50, 60, 99 |
| `smto_kw` | `string` | `varchar` |  |  | Ship Materials To (keyword). Allowed values: tsmdm.smto.ship.to, tsmdm.smto.location, tsmdm.smto.owner, tsmdm.smto.in.use.by, tsmdm.smto.dealer, tsmdm.smto.subcontractor, tsmdm.smto.not.applicable |
| `micn` | `integer` | `int` |  |  | Material Issue Constraint. Allowed values: 10, 20, 100 |
| `micn_kw` | `string` | `varchar` |  |  | Material Issue Constraint (keyword). Allowed values: tstdm.micn.order.complete, tstdm.micn.act.complete, tstdm.micn.none |
| `wcyn` | `integer` | `int` |  |  | Warranty Claim. Allowed values: 1, 2 |
| `wcyn_kw` | `string` | `varchar` |  |  | Warranty Claim (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clmt` | `integer` | `int` |  |  | Claim Method. Allowed values: 10, 20, 30 |
| `clmt_kw` | `string` | `varchar` |  |  | Claim Method (keyword). Allowed values: tsmdm.clmt.na, tsmdm.clmt.material, tsmdm.clmt.costs |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `cdis` | `string` | `varchar` |  |  | Cancel Reason. Max length: 6 |
| `cadt` | `date` | `date` |  |  | Cancel Date |
| `lutd` | `timestamp` | `timestamp_ntz` |  |  | Last Transaction Date |
| `ract` | `timestamp` | `timestamp_ntz` |  |  | Reaction (time) |
| `orrt` | `timestamp` | `timestamp_ntz` |  |  | Order Release Time |
| `cstp` | `string` | `varchar` |  |  | Service Type. Max length: 3 |
| `cctp` | `string` | `varchar` |  |  | Coverage Type. Max length: 3 |
| `odpr` | `integer` | `int` |  |  | Order Procedure. Allowed values: 0, 5, 10, 15 |
| `odpr_kw` | `string` | `varchar` |  |  | Order Procedure (keyword). Allowed values: , tsmdm.odpr.normal, tsmdm.odpr.emergency, tsmdm.odpr.rental |
| `orac` | `integer` | `int` |  |  | Order Accepted. Allowed values: 0, 1, 2 |
| `orac_kw` | `string` | `varchar` |  |  | Order Accepted (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `cvtm` | `timestamp` | `timestamp_ntz` |  |  | Coverage Time |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `vtbo` | `integer` | `int` |  |  | VAT Based On. Allowed values: 0, 5, 10 |
| `vtbo_kw` | `string` | `varchar` |  |  | VAT Based On (keyword). Allowed values: , tsmdm.vtbo.services, tsmdm.vtbo.goods |
| `styp` | `string` | `varchar` |  |  | Sales Type. Max length: 6 |
| `logn` | `string` | `varchar` |  |  | Login Code. Max length: 16 |
| `trpm` | `integer` | `int` |  |  | Travel Planning Method. Allowed values: 1, 2, 3 |
| `trpm_kw` | `string` | `varchar` |  |  | Travel Planning Method (keyword). Allowed values: tsmdm.trpm.header, tsmdm.trpm.activity.line, tsmdm.trpm.not.applicable |
| `ctcs` | `integer` | `int` |  |  | Travel Total Line for Distance and Time. Allowed values: 1, 2 |
| `ctcs_kw` | `string` | `varchar` |  |  | Travel Total Line for Distance and Time (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ucce` | `integer` | `int` |  |  | Use Coverage Calculation for Estimates. Allowed values: 1, 2 |
| `ucce_kw` | `string` | `varchar` |  |  | Use Coverage Calculation for Estimates (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `utce` | `integer` | `int` |  |  | Use Tax for Estimates. Allowed values: 1, 2 |
| `utce_kw` | `string` | `varchar` |  |  | Use Tax for Estimates (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inpl` | `string` | `varchar` |  |  | Installment Plan. Max length: 9 |
| `insm` | `integer` | `int` |  |  | Installment Method. Allowed values: 10, 20, 100 |
| `insm_kw` | `string` | `varchar` |  |  | Installment Method (keyword). Allowed values: tcmdm.insm.header, tcmdm.insm.activity.line, tcmdm.insm.not.applicable |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Acceptance Date |
| `dipy` | `integer` | `int` |  |  | Discount Policy. Allowed values: 1, 2 |
| `dipy_kw` | `string` | `varchar` |  |  | Discount Policy (keyword). Allowed values: tsmdm.dipy.bfcv, tsmdm.dipy.afcv |
| `dfpb` | `integer` | `int` |  |  | Discounts from Price Books. Allowed values: 1, 2 |
| `dfpb_kw` | `string` | `varchar` |  |  | Discounts from Price Books (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `rcde` | `string` | `varchar` |  |  | Receipt Delivery Terms. Max length: 3 |
| `rptp` | `string` | `varchar` |  |  | Receipt Point of Title Passage. Max length: 9 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `wrtp` | `integer` | `int` |  |  | Warranty Type. Allowed values: 5, 10, 15 |
| `wrtp_kw` | `string` | `varchar` |  |  | Warranty Type (keyword). Allowed values: tstdm.wrtp.serial, tstdm.wrtp.general, tstdm.wrtp.no.warranty |
| `wrty` | `string` | `varchar` |  |  | Warranty. Max length: 16 |
| `qtno` | `string` | `varchar` |  |  | Quote. Max length: 9 |
| `revn` | `integer` | `int` |  |  | Quote Revision |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `pbsm` | `integer` | `int` |  |  | Problem Solving Method. Allowed values: 10, 20, 30, 90, 100 |
| `pbsm_kw` | `string` | `varchar` |  |  | Problem Solving Method (keyword). Allowed values: tcmcs.pbsm.8d, tcmcs.pbsm.a3, tcmcs.pbsm.chlt, tcmcs.pbsm.other, tcmcs.pbsm.na |
| `prac` | `integer` | `int` |  |  | Acknowledgement Print Status. Allowed values: 10, 20, 30 |
| `prac_kw` | `string` | `varchar` |  |  | Acknowledgement Print Status (keyword). Allowed values: tssoc.prst.not.required, tssoc.prst.to.be.printed, tssoc.prst.printed |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `npeg` | `integer` | `int` |  |  | Handle Customer Owned Project Pegged Items as Not Pegged. Allowed values: 1, 2 |
| `npeg_kw` | `string` | `varchar` |  |  | Handle Customer Owned Project Pegged Items as Not Pegged (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aril` | `integer` | `int` |  |  | Actual and Invoice Lines. Allowed values: 10, 20, 30, 40 |
| `aril_kw` | `string` | `varchar` |  |  | Actual and Invoice Lines (keyword). Allowed values: tstdm.aril.no, tstdm.aril.new.created, tstdm.aril.converted, tstdm.aril.conv.after.inv |
| `rrqt` | `integer` | `int` |  |  | Rental Period |
| `tmun` | `integer` | `int` |  |  | Rental Period Unit. Allowed values: 10, 20, 30, 40, 50, 90 |
| `tmun_kw` | `string` | `varchar` |  |  | Rental Period Unit (keyword). Allowed values: tctmun.hours, tctmun.days, tctmun.weeks, tctmun.months, tctmun.years, tctmun.not.applicable |
| `orqt` | `integer` | `int` |  |  | Original Rental Period |
| `otmn` | `integer` | `int` |  |  | Original Rental Period Unit. Allowed values: 10, 20, 30, 40, 50, 90 |
| `otmn_kw` | `string` | `varchar` |  |  | Original Rental Period Unit (keyword). Allowed values: tctmun.hours, tctmun.days, tctmun.weeks, tctmun.months, tctmun.years, tctmun.not.applicable |
| `ahpt` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 5, 7, 10, 15, 20, 90 |
| `ahpt_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcpgtp.bp.based, tcpgtp.bploc.based, tcpgtp.order.based, tcpgtp.cust.ref.based, tcpgtp.int.ref.based, tcpgtp.not.appl |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `txyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `txyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adpt` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `rsrp` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `rsrp_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `atpc` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `atpc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `atpd` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Order Text |
| `txtb` | `integer` | `int` |  |  | Invoice Text |
| `txtc` | `integer` | `int` |  |  | Cancel Text |
| `clst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsbsc100 Installation Group |
| `item_sern_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `item_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `lcad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `fdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `csar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm105 Service Areas |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `ccar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm145 Service Cars |
| `crit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `cplt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `pcon_pcch_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `pcon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `csqu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tssoc100 Service Order Quotes |
| `bppr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `pldd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `camr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `caro_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cstp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm030 Service Types |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `styp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs202 Sales Types |
| `inpl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs245 Installment Plans |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `rcde_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `rptp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `qtno_revn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsepp100 Quotes |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `trdi_buln` | `float` | `float` |  |  | CC: Travel Distance in Base Unit Length |
| `oamt_homc` | `float` | `float` |  |  | CC: Estimated Order Amount in Local Currency |
| `oamt_rpc1` | `float` | `float` |  |  | CC: Estimated Order Amount in Reporting Currency 1 |
| `oamt_rpc2` | `float` | `float` |  |  | CC: Estimated Order Amount in Reporting Currency 2 |
| `oamt_refc` | `float` | `float` |  |  | CC: Estimated Order Amount in Reference Currency |
| `oamt_dwhc` | `float` | `float` |  |  | CC: Estimated Order Amount in Data Warehouse Currency |
| `amnt_homc` | `float` | `float` |  |  | CC: Actual Order Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Actual Order Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Actual Order Amount in Reporting Currency 2 |
| `amnt_refc` | `float` | `float` |  |  | CC: Actual Order Amount in Reference Currency |
| `amnt_dwhc` | `float` | `float` |  |  | CC: Actual Order Amount in Data Warehouse Currency |
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

# tssoc210

LN tssoc210 Service Order Activities table, Generated 2026-04-10T19:42:38Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tssoc210` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `acln` |
| **Column count** | 250 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Service Order. Max length: 9 |
| `acln` | `integer` | `int` | `PK` | `not_null` | (required) Activity Line Number |
| `crac` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `sear__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Argument - base language. Max length: 16 |
| `clst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `lcad` | `string` | `varchar` |  |  | Location Address. Max length: 9 |
| `cstp` | `string` | `varchar` |  |  | Service Type. Max length: 3 |
| `actg` | `string` | `varchar` |  |  | Activity Group. Max length: 6 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `sern` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `ssta` | `integer` | `int` |  |  | Serial Status. Allowed values: 1, 2, 3, 4, 5, 6, 9, 15, 99 |
| `ssta_kw` | `string` | `varchar` |  |  | Serial Status (keyword). Allowed values: tscfg.cfst.stup, tscfg.cfst.actv, tscfg.cfst.revi, tscfg.cfst.dftv, tscfg.cfst.wkcn, tscfg.cfst.tbrc, tscfg.cfst.removed, tscfg.cfst.superseded, tscfg.cfst.nota |
| `emno` | `string` | `varchar` |  |  | Preferred Engineer. Max length: 9 |
| `ccar` | `string` | `varchar` |  |  | Service Car. Max length: 10 |
| `orac` | `string` | `varchar` |  |  | Derived from Planned Activity. Max length: 9 |
| `oras` | `integer` | `int` |  |  | Derived from Planned Activity Sequence Number |
| `ccll` | `string` | `varchar` |  |  | Call. Max length: 9 |
| `fcon` | `string` | `varchar` |  |  | Field Change Order. Max length: 9 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `pcac` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `inby` | `integer` | `int` |  |  | Invoicing By. Allowed values: 5, 10, 99 |
| `inby_kw` | `string` | `varchar` |  |  | Invoicing By (keyword). Allowed values: tcinby.project, tcinby.service, tcinby.not.appl |
| `cpor` | `integer` | `int` |  |  | Project Peg Origin. Allowed values: 5, 10, 15, 100 |
| `cpor_kw` | `string` | `varchar` |  |  | Project Peg Origin (keyword). Allowed values: tcpeg.cpor.top.demand, tcpeg.cpor.costing.break, tcpeg.cpor.manual, tcpeg.cpor.not.applicable |
| `cvtm` | `timestamp` | `timestamp_ntz` |  |  | Coverage Time |
| `crtc` | `string` | `varchar` |  |  | Response Type. Max length: 3 |
| `prpr` | `integer` | `int` |  |  | Problem Priority |
| `cupr` | `integer` | `int` |  |  | Customer Priority |
| `obpr` | `integer` | `int` |  |  | Serialized Item Priority |
| `cprl` | `string` | `varchar` |  |  | Problem. Max length: 10 |
| `cstn` | `string` | `varchar` |  |  | Solution. Max length: 10 |
| `asta` | `integer` | `int` |  |  | Activity Status. Allowed values: 5, 10, 15, 20, 23, 25, 35, 40 |
| `asta_kw` | `string` | `varchar` |  |  | Activity Status (keyword). Allowed values: tssoc.osta.free, tssoc.osta.planned, tssoc.osta.released, tssoc.osta.completed, tssoc.osta.costed, tssoc.osta.closed, tssoc.osta.canceled, tssoc.osta.rejected |
| `subc` | `integer` | `int` |  |  | Subcontracted. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Subcontracted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `supm` | `integer` | `int` |  |  | Supply Material to Subcontractor. Allowed values: 1, 2 |
| `supm_kw` | `string` | `varchar` |  |  | Supply Material to Subcontractor (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crpu` | `integer` | `int` |  |  | Create Purchase Requisition. Allowed values: 1, 2 |
| `crpu_kw` | `string` | `varchar` |  |  | Create Purchase Requisition (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `sbit` | `string` | `varchar` |  |  | Item Subcontracting. Max length: 47 |
| `cmea` | `string` | `varchar` |  |  | Measurement Type. Max length: 6 |
| `mepo` | `string` | `varchar` |  |  | Position. Max length: 8 |
| `cskt` | `string` | `varchar` |  |  | Service Kit. Max length: 6 |
| `cchl` | `integer` | `int` |  |  | Checklist |
| `pdvl` | `float` | `float` |  |  | Expected Measurement Value |
| `mvln` | `float` | `float` |  |  | Measured Value Numeric |
| `mvla` | `integer` | `int` |  |  | Measured Value Alphanumeric |
| `come` | `string` | `varchar` |  |  | Main Counter. Max length: 6 |
| `cova` | `float` | `float` |  |  | Main Counter Value |
| `dbfc` | `float` | `float` |  |  | Downtime Ratio |
| `acdu` | `float` | `float` |  |  | Activity Duration |
| `trdu` | `float` | `float` |  |  | Travel Duration |
| `trdi` | `float` | `float` |  |  | Travel Distance |
| `rfpt` | `string` | `varchar` |  |  | Reference Point. Max length: 6 |
| `adtm` | `integer` | `int` |  |  | Actual Down Time |
| `appo` | `integer` | `int` |  |  | Appointment. Allowed values: 1, 2 |
| `appo_kw` | `string` | `varchar` |  |  | Appointment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pstm` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Time |
| `pftm` | `timestamp` | `timestamp_ntz` |  |  | Planned Finish Time |
| `ptst` | `timestamp` | `timestamp_ntz` |  |  | Planned Travel Start Time |
| `ptft` | `timestamp` | `timestamp_ntz` |  |  | Planned Travel Finish Time |
| `ostm` | `timestamp` | `timestamp_ntz` |  |  | Original Planned Start Time |
| `oftm` | `timestamp` | `timestamp_ntz` |  |  | Original Planned Finish Time |
| `estm` | `timestamp` | `timestamp_ntz` |  |  | Earliest Start Time |
| `lftm` | `timestamp` | `timestamp_ntz` |  |  | Latest Finish Time |
| `astm` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Time |
| `aftm` | `timestamp` | `timestamp_ntz` |  |  | Actual Finish Time |
| `csar` | `string` | `varchar` |  |  | Service Area. Max length: 3 |
| `cctp` | `string` | `varchar` |  |  | Coverage Type. Max length: 3 |
| `csco` | `string` | `varchar` |  |  | Service Contract. Max length: 9 |
| `pmtd` | `integer` | `int` |  |  | Pricing Method. Allowed values: 5, 10, 12, 15, 25, 30 |
| `pmtd_kw` | `string` | `varchar` |  |  | Pricing Method (keyword). Allowed values: tsmdm.pmtd.not.applicable, tsmdm.pmtd.fixed.act.pr, tsmdm.pmtd.fixed.order.pr, tsmdm.pmtd.time.material, tsmdm.pmtd.quo.fixed, tsmdm.pmtd.quo.fixedplus |
| `pcon` | `string` | `varchar` |  |  | Pricing Contract. Max length: 9 |
| `pcch` | `integer` | `int` |  |  | Pricing Contract Change |
| `pcln` | `integer` | `int` |  |  | Pricing Contract Line |
| `espr` | `float` | `float` |  |  | Estimated Sales Price |
| `pris` | `float` | `float` |  |  | Sales Price |
| `eaod` | `float` | `float` |  |  | Estimated Order Discount Amount |
| `amod` | `float` | `float` |  |  | Order Discount Amount |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 5, 10, 15, 20, 25, 30, 35, 45, 50, 55, 60, 99 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: tstdm.porg.manual, tstdm.porg.other, tstdm.porg.serv.contract, tstdm.porg.master.routing, tstdm.porg.routing.option, tstdm.porg.ref.activity, tstdm.porg.trav.rate.book, tstdm.porg.item.serv, tstdm.porg.def.pr.book, tstdm.porg.price.structure, tstdm.porg.serial, tstdm.porg.not.applicable |
| `vtbo` | `integer` | `int` |  |  | VAT Based On. Allowed values: 0, 5, 10 |
| `vtbo_kw` | `string` | `varchar` |  |  | VAT Based On (keyword). Allowed values: , tsmdm.vtbo.services, tsmdm.vtbo.goods |
| `camr` | `string` | `varchar` |  |  | Master Routing. Max length: 16 |
| `caro_1` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_2` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_3` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_4` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_5` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_6` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_7` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_8` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_9` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `caro_10` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `orps` | `integer` | `int` |  |  | Master Routing Operation |
| `cdis` | `string` | `varchar` |  |  | Cancel Reason. Max length: 6 |
| `cadt` | `date` | `date` |  |  | Cancel Date |
| `wrtp` | `integer` | `int` |  |  | Warranty Type. Allowed values: 5, 10, 15 |
| `wrtp_kw` | `string` | `varchar` |  |  | Warranty Type (keyword). Allowed values: tstdm.wrtp.serial, tstdm.wrtp.general, tstdm.wrtp.no.warranty |
| `wrty` | `string` | `varchar` |  |  | Warranty. Max length: 16 |
| `ains` | `integer` | `int` |  |  | Apply Installments. Allowed values: 1, 2 |
| `ains_kw` | `string` | `varchar` |  |  | Apply Installments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inpl` | `string` | `varchar` |  |  | Installment Plan. Max length: 9 |
| `acdt` | `timestamp` | `timestamp_ntz` |  |  | Acceptance Date |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `smto` | `integer` | `int` |  |  | Ship Materials To. Allowed values: 10, 20, 30, 40, 50, 60, 99 |
| `smto_kw` | `string` | `varchar` |  |  | Ship Materials To (keyword). Allowed values: tsmdm.smto.ship.to, tsmdm.smto.location, tsmdm.smto.owner, tsmdm.smto.in.use.by, tsmdm.smto.dealer, tsmdm.smto.subcontractor, tsmdm.smto.not.applicable |
| `micn` | `integer` | `int` |  |  | Material Issue Constraint. Allowed values: 10, 20, 100 |
| `micn_kw` | `string` | `varchar` |  |  | Material Issue Constraint (keyword). Allowed values: tstdm.micn.order.complete, tstdm.micn.act.complete, tstdm.micn.none |
| `adfa` | `integer` | `int` |  |  | Adjust Dates from Assignments. Allowed values: 1, 2 |
| `adfa_kw` | `string` | `varchar` |  |  | Adjust Dates from Assignments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `pbsm` | `integer` | `int` |  |  | Problem Solving Method. Allowed values: 10, 20, 30, 90, 100 |
| `pbsm_kw` | `string` | `varchar` |  |  | Problem Solving Method (keyword). Allowed values: tcmcs.pbsm.8d, tcmcs.pbsm.a3, tcmcs.pbsm.chlt, tcmcs.pbsm.other, tcmcs.pbsm.na |
| `docn` | `string` | `varchar` |  |  | Turnaround Time Document. Max length: 9 |
| `qtno` | `string` | `varchar` |  |  | Quote. Max length: 9 |
| `revn` | `integer` | `int` |  |  | Quote Revision |
| `qtnl` | `integer` | `int` |  |  | Quote Line |
| `altn` | `integer` | `int` |  |  | Quote Line Alternative |
| `ityn` | `integer` | `int` |  |  | Interrupted for Quote. Allowed values: 1, 2 |
| `ityn_kw` | `string` | `varchar` |  |  | Interrupted for Quote (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asgd` | `string` | `varchar` |  |  | After Sales Service Line ID. Max length: 22 |
| `prac` | `integer` | `int` |  |  | Acknowledgement Print Status. Allowed values: 10, 20, 30 |
| `prac_kw` | `string` | `varchar` |  |  | Acknowledgement Print Status (keyword). Allowed values: tssoc.prst.not.required, tssoc.prst.to.be.printed, tssoc.prst.printed |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `rnyn` | `integer` | `int` |  |  | Rental. Allowed values: 1, 2 |
| `rnyn_kw` | `string` | `varchar` |  |  | Rental (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `erdt` | `timestamp` | `timestamp_ntz` |  |  | Expected Return Time |
| `rrqt` | `integer` | `int` |  |  | Rental Period |
| `tmun` | `integer` | `int` |  |  | Rental Period Unit. Allowed values: 10, 20, 30, 40, 50, 90 |
| `tmun_kw` | `string` | `varchar` |  |  | Rental Period Unit (keyword). Allowed values: tctmun.hours, tctmun.days, tctmun.weeks, tctmun.months, tctmun.years, tctmun.not.applicable |
| `orqt` | `integer` | `int` |  |  | Original Rental Period |
| `otmn` | `integer` | `int` |  |  | Original Rental Period Unit. Allowed values: 10, 20, 30, 40, 50, 90 |
| `otmn_kw` | `string` | `varchar` |  |  | Original Rental Period Unit (keyword). Allowed values: tctmun.hours, tctmun.days, tctmun.weeks, tctmun.months, tctmun.years, tctmun.not.applicable |
| `urrp` | `integer` | `int` |  |  | Use Order Rental Times. Allowed values: 1, 2 |
| `urrp_kw` | `string` | `varchar` |  |  | Use Order Rental Times (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `xrnh` | `integer` | `int` |  |  | External Hire. Allowed values: 1, 2 |
| `xrnh_kw` | `string` | `varchar` |  |  | External Hire (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `xrrt` | `float` | `float` |  |  | External Hire Rate |
| `cuxr` | `string` | `varchar` |  |  | External Hire Rate Unit. Max length: 3 |
| `xrcu` | `string` | `varchar` |  |  | External Hire Currency. Max length: 3 |
| `xrsr` | `string` | `varchar` |  |  | External Hire Serial Number. Max length: 30 |
| `bsch` | `string` | `varchar` |  |  | Billing Schedule. Max length: 9 |
| `atbl` | `integer` | `int` |  |  | Automatic Billing. Allowed values: 1, 2 |
| `atbl_kw` | `string` | `varchar` |  |  | Automatic Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `noun` | `integer` | `int` |  |  | Number of Units |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `rltm` | `timestamp` | `timestamp_ntz` |  |  | Planned Release Time |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 40 |
| `corp__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order Position - base language. Max length: 16 |
| `cors__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order Sequence - base language. Max length: 11 |
| `skla` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `sklb` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `sklc` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `aivl` | `float` | `float` |  |  | Obsolete |
| `nivl` | `float` | `float` |  |  | Obsolete |
| `advl` | `float` | `float` |  |  | Obsolete |
| `atpc` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `atpc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `atpd` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Activity Text |
| `txtb` | `integer` | `int` |  |  | Call Text |
| `txtc` | `integer` | `int` |  |  | Solution Text |
| `txtd` | `integer` | `int` |  |  | Cancel Text |
| `orno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tssoc200 Service Orders |
| `crac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `clst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsbsc100 Installation Group |
| `lcad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cstp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm030 Service Types |
| `actg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm010 Activity Groups |
| `item_sern_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `item_xrsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `ccar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm145 Service Cars |
| `ccll_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm100 Calls |
| `fcon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tssoc500 Field Change Order Headers |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `crtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm020 Response Types |
| `prpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `cupr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `obpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `cprl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm330 Problems |
| `cstn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm335 Solutions |
| `bfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `sbit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cmea_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm065 Measurement Types |
| `mepo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm215 Positions |
| `cskt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm110 Service Kits |
| `cchl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm040 Checklists |
| `come_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm065 Measurement Types |
| `csar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm105 Service Areas |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `pcon_pcch_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `pcon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `camr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `cdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `inpl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs245 Installment Plans |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `docn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcttm100 Turnaround Time Documents |
| `qtno_revn_qtnl_altn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsepp110 Quote Lines |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cuxr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `xrcu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `bsch_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm025 Billing Schedules |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `trdi_buln` | `float` | `float` |  |  | CC: Travel Distance in Base Unit Length |
| `pris_dwhc` | `float` | `float` |  |  | CC: Sales Price in Data Warehouse Currency |
| `pris_homc` | `float` | `float` |  |  | CC: Sales Price in Local Currency |
| `pris_rpc1` | `float` | `float` |  |  | CC: Sales Price in Reporting Currency 1 |
| `pris_rpc2` | `float` | `float` |  |  | CC: Sales Price in Reporting Currency 2 |
| `pris_refc` | `float` | `float` |  |  | CC: Sales Price in Reference Currency |
| `eaod_dwhc` | `float` | `float` |  |  | CC: Estimated Order Discount Amount in Data Warehouse Currency |
| `eaod_homc` | `float` | `float` |  |  | CC: Estimated Order Discount Amount in Local Currency |
| `eaod_rpc1` | `float` | `float` |  |  | CC: Estimated Order Discount Amount in Reporting Currency 1 |
| `eaod_rpc2` | `float` | `float` |  |  | CC: Estimated Order Discount Amount in Reporting Currency 2 |
| `eaod_refc` | `float` | `float` |  |  | CC: Estimated Order Discount Amount in Reference Currency |
| `amod_dwhc` | `float` | `float` |  |  | CC: Order Discount Amount in Data Warehouse Currency |
| `amod_homc` | `float` | `float` |  |  | CC: Order Discount Amount in Local Currency |
| `amod_rpc1` | `float` | `float` |  |  | CC: Order Discount Amount in Reporting Currency 1 |
| `amod_rpc2` | `float` | `float` |  |  | CC: Order Discount Amount in Reporting Currency 2 |
| `amod_refc` | `float` | `float` |  |  | CC: Order Discount Amount in Reference Currency |
| `espr_dwhc` | `float` | `float` |  |  | CC: Estimated Sales Price in Data Warehouse Currency |
| `espr_homc` | `float` | `float` |  |  | CC: Estimated Sales Price in Local Currency |
| `espr_rpc1` | `float` | `float` |  |  | CC: Estimated Sales Price in Reporting Currency 1 |
| `espr_rpc2` | `float` | `float` |  |  | CC: Estimated Sales Price in Reporting Currency 2 |
| `espr_refc` | `float` | `float` |  |  | CC: Estimated Sales Price in Reference Currency |
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

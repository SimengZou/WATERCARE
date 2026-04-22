# tdsls301

LN tdsls301 Sales Contract Lines table, Generated 2026-04-10T19:41:24Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsls301` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `pono`, `cofc` |
| **Column count** | 260 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Sales Office. Max length: 6 |
| `adco` | `integer` | `int` |  |  | Action on Deviating Customer Order. Allowed values: 5, 10, 15, 20 |
| `adco_kw` | `string` | `varchar` |  |  | Action on Deviating Customer Order (keyword). Allowed values: tdsls.adco.not.applicable, tdsls.adco.block, tdsls.adco.cont.release, tdsls.adco.cont.contract |
| `aeco` | `integer` | `int` |  |  | Action on Deviating Empty Customer Order. Allowed values: 5, 10, 15, 20 |
| `aeco_kw` | `string` | `varchar` |  |  | Action on Deviating Empty Customer Order (keyword). Allowed values: tdsls.adco.not.applicable, tdsls.adco.block, tdsls.adco.cont.release, tdsls.adco.cont.contract |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 30 |
| `ccrf__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Contract Reference - base language. Max length: 30 |
| `icyp` | `integer` | `int` |  |  | Contract Type. Allowed values: 1, 2 |
| `icyp_kw` | `string` | `varchar` |  |  | Contract Type (keyword). Allowed values: tcicyp.year, tcicyp.proj |
| `citt` | `string` | `varchar` |  |  | Item Code System. Max length: 3 |
| `citm__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item - base language. Max length: 35 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpgs` | `string` | `varchar` |  |  | Sales Price Group. Max length: 6 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `stsi` | `string` | `varchar` |  |  | Ship-to Site. Max length: 9 |
| `stwh` | `string` | `varchar` |  |  | Ship-to Warehouse. Max length: 6 |
| `imcs` | `string` | `varchar` |  |  | Intermediate Consignee. Max length: 35 |
| `dlpt` | `string` | `varchar` |  |  | Delivery Point. Max length: 9 |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Contract Date |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `thic` | `float` | `float` |  |  | Height |
| `qoor` | `float` | `float` |  |  | Agreed Quantity |
| `qomn` | `float` | `float` |  |  | Minimum Quantity |
| `qomx` | `float` | `float` |  |  | Maximum Quantity |
| `aemq` | `integer` | `int` |  |  | Action on Exceeding Maximum Quantity. Allowed values: 1, 2, 3 |
| `aemq_kw` | `string` | `varchar` |  |  | Action on Exceeding Maximum Quantity (keyword). Allowed values: tdsls.imae.take.contract, tdsls.imae.ask.for.it, tdsls.imae.skip.contract |
| `mqtl` | `float` | `float` |  |  | Maximum Quantity Tolerance |
| `iqab` | `integer` | `int` |  |  | Quantity Binding. Allowed values: 1, 2 |
| `iqab_kw` | `string` | `varchar` |  |  | Quantity Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qimo` | `float` | `float` |  |  | Minimum Order Quantity |
| `cuqs` | `string` | `varchar` |  |  | Sales Unit. Max length: 3 |
| `cvqs` | `float` | `float` |  |  | Order Unit Conversion Factor |
| `priu` | `integer` | `int` |  |  | Contract Prices in Use. Allowed values: 1, 2 |
| `priu_kw` | `string` | `varchar` |  |  | Contract Prices in Use (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pric` | `float` | `float` |  |  | Price |
| `prbo` | `string` | `varchar` |  |  | Price Book. Max length: 9 |
| `disc_1` | `float` | `float` |  |  | Discount |
| `disc_2` | `float` | `float` |  |  | Discount |
| `disc_3` | `float` | `float` |  |  | Discount |
| `disc_4` | `float` | `float` |  |  | Discount |
| `disc_5` | `float` | `float` |  |  | Discount |
| `disc_6` | `float` | `float` |  |  | Discount |
| `disc_7` | `float` | `float` |  |  | Discount |
| `disc_8` | `float` | `float` |  |  | Discount |
| `disc_9` | `float` | `float` |  |  | Discount |
| `disc_10` | `float` | `float` |  |  | Discount |
| `disc_11` | `float` | `float` |  |  | Discount |
| `ldam_1` | `float` | `float` |  |  | Discount Amount |
| `ldam_2` | `float` | `float` |  |  | Discount Amount |
| `ldam_3` | `float` | `float` |  |  | Discount Amount |
| `ldam_4` | `float` | `float` |  |  | Discount Amount |
| `ldam_5` | `float` | `float` |  |  | Discount Amount |
| `ldam_6` | `float` | `float` |  |  | Discount Amount |
| `ldam_7` | `float` | `float` |  |  | Discount Amount |
| `ldam_8` | `float` | `float` |  |  | Discount Amount |
| `ldam_9` | `float` | `float` |  |  | Discount Amount |
| `ldam_10` | `float` | `float` |  |  | Discount Amount |
| `ldam_11` | `float` | `float` |  |  | Discount Amount |
| `cdis_1` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_2` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_3` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_4` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_5` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_6` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_7` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_8` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_9` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_10` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_11` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `dsor` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3, 4, 5 |
| `dsor_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tdpcg.dsor.stnd, tdpcg.dsor.socn, tdpcg.dsor.pocn, tdpcg.dsor.rfqo, tdpcg.dsor.prom |
| `dssc_1` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_2` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_3` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_4` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_5` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_6` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_7` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_8` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_9` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_10` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_11` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `cpwc` | `integer` | `int` |  |  | Calculate Price with Cumulative. Allowed values: 1, 2 |
| `cpwc_kw` | `string` | `varchar` |  |  | Calculate Price with Cumulative (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rpds` | `integer` | `int` |  |  | Redetermine Price and Discounts at Shipment. Allowed values: 1, 2 |
| `rpds_kw` | `string` | `varchar` |  |  | Redetermine Price and Discounts at Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dmth_1` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_1` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_2` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_2` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_3` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_3` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_4` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_4` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_5` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_5` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_6` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_6` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_7` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_7` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_8` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_8` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_9` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_9` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_10` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_10` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_11` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_11` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dtrm` | `integer` | `int` |  |  | Determining. Allowed values: 1, 2 |
| `dtrm_kw` | `string` | `varchar` |  |  | Determining (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elgb` | `integer` | `int` |  |  | Eligible. Allowed values: 1, 2 |
| `elgb_kw` | `string` | `varchar` |  |  | Eligible (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | Business Partner Tax Country. Max length: 3 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `qicl` | `float` | `float` |  |  | Called Quantity |
| `camt` | `float` | `float` |  |  | Called Amount |
| `qiiv` | `float` | `float` |  |  | Invoiced Quantity |
| `bamt` | `float` | `float` |  |  | Invoiced Amount |
| `ieva` | `integer` | `int` |  |  | Evaluation [y/n/repeat]. Allowed values: 1, 2, 3 |
| `ieva_kw` | `string` | `varchar` |  |  | Evaluation [y/n/repeat] (keyword). Allowed values: tcieva.yes, tcieva.no, tcieva.repeat |
| `cups` | `string` | `varchar` |  |  | Sales Price Unit. Max length: 3 |
| `cvps` | `float` | `float` |  |  | Price Unit Conversion Factor |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `aeed` | `integer` | `int` |  |  | Action on Exceeding Expiry Date. Allowed values: 1, 2, 3 |
| `aeed_kw` | `string` | `varchar` |  |  | Action on Exceeding Expiry Date (keyword). Allowed values: tdsls.imae.take.contract, tdsls.imae.ask.for.it, tdsls.imae.skip.contract |
| `dttl` | `integer` | `int` |  |  | Expiry Date Tolerance |
| `hqan` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `hqan_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tdpur.hval.hold, tdpur.hval.continue |
| `hval` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `hval_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tdpur.hval.hold, tdpur.hval.continue |
| `icap` | `integer` | `int` |  |  | Contract Line Status. Allowed values: 1, 2, 3 |
| `icap_kw` | `string` | `varchar` |  |  | Contract Line Status (keyword). Allowed values: tcicap.free, tcicap.active, tcicap.closed |
| `maxv` | `float` | `float` |  |  | Obsolete |
| `minv` | `float` | `float` |  |  | Obsolete |
| `owno__en_us` | `string` | `varchar` |  | `not_null` | (required) Obsolete - base language. Max length: 30 |
| `revi` | `string` | `varchar` |  |  | Engineering Item. Max length: 6 |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `ceno` | `string` | `varchar` |  |  | Exempt Certificate. Max length: 24 |
| `afrb` | `integer` | `int` |  |  | Item applicable for Retro-Billing. Allowed values: 1, 2, 3 |
| `afrb_kw` | `string` | `varchar` |  |  | Item applicable for Retro-Billing (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `pcpr` | `integer` | `int` |  |  | Retro-Billed Price Changes present. Allowed values: 1, 2 |
| `pcpr_kw` | `string` | `varchar` |  |  | Retro-Billed Price Changes present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcod` | `string` | `varchar` |  |  | Exempt Reason. Max length: 6 |
| `dest` | `integer` | `int` |  |  | Goods Flow. Allowed values: 1, 2, 3, 4 |
| `dest_kw` | `string` | `varchar` |  |  | Goods Flow (keyword). Allowed values: tdsls.dest.warehousing, tdsls.dest.sales, tdsls.dest.transfer, tdsls.dest.direct.delivery |
| `bfbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `crep` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `osrp` | `string` | `varchar` |  |  | External Sales Representative. Max length: 9 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `adcs` | `string` | `varchar` |  |  | Additional Cost Set. Max length: 3 |
| `ucms` | `integer` | `int` |  |  | Use Extended Confirmation Logic for Schedules. Allowed values: 1, 2 |
| `ucms_kw` | `string` | `varchar` |  |  | Use Extended Confirmation Logic for Schedules (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bdcl` | `string` | `varchar` |  |  | Credit Limit Blocking Definition. Max length: 3 |
| `pbor` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2, 3, 4 |
| `pbor_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdpcg.pbor.stnd, tdpcg.pbor.socn, tdpcg.pbor.pocn, tdpcg.pbor.rfqo |
| `txta` | `integer` | `int` |  |  | Contract Line Text |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls300 Sales Contracts |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `item_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa001 Item - Sales |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cpgs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_dlpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom134 Delivery Points |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `stwh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cuqs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `prbo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg011 Price Books |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `cups_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `bfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `osrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `adcs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls024 Sales Additional Cost Sets |
| `bdcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs211 Blocking Definitions |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `qoor_invu` | `float` | `float` |  |  | CC: Sales Contract Line Agreed Quantity in Inventory Unit |
| `qoor_buar` | `float` | `float` |  |  | CC: Sales Contract Line Agreed Quantity in Base Unit Area |
| `qoor_buln` | `float` | `float` |  |  | CC: Sales Contract Line Agreed Quantity in Base Unit Length |
| `qoor_bupc` | `float` | `float` |  |  | CC: Sales Contract Line Agreed Quantity in Base Unit Piece |
| `qoor_butm` | `float` | `float` |  |  | CC: Sales Contract Line Agreed Quantity in Base Unit Time |
| `qoor_buvl` | `float` | `float` |  |  | CC: Sales Contract Line Agreed Quantity in Base Unit Volume |
| `qoor_buwg` | `float` | `float` |  |  | CC: Sales Contract Line Agreed Quantity in Base Unit Weight |
| `qicl_trnu` | `float` | `float` |  |  | CC: Sales Contract Line Called Quantity in Transaction Unit |
| `qicl_buar` | `float` | `float` |  |  | CC: Sales Contract Line Called Quantity in Base Unit Area |
| `qicl_buln` | `float` | `float` |  |  | CC: Sales Contract Line Called Quantity in Base Unit Length |
| `qicl_bupc` | `float` | `float` |  |  | CC: Sales Contract Line Called Quantity in Base Unit Piece |
| `qicl_butm` | `float` | `float` |  |  | CC: Sales Contract Line Called Quantity in Base Unit Time |
| `qicl_buvl` | `float` | `float` |  |  | CC: Sales Contract Line Called Quantity in Base Unit Volume |
| `qicl_buwg` | `float` | `float` |  |  | CC: Sales Contract Line Called Quantity in Base Unit Weight |
| `qiiv_trnu` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Quantity in Transaction Unit |
| `qiiv_buar` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Quantity in Base Unit Area |
| `qiiv_buln` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Quantity in Base Unit Length |
| `qiiv_bupc` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Quantity in Base Unit Piece |
| `qiiv_butm` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Quantity in Base Unit Time |
| `qiiv_buvl` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Quantity in Base Unit Volume |
| `qiiv_buwg` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Quantity in Base Unit Weight |
| `camt_lclc` | `float` | `float` |  |  | CC: Sales Contract Line Called Amount in Local Currency |
| `camt_rpc1` | `float` | `float` |  |  | CC: Sales Contract Line Called Amount in Reporting Currency 1 |
| `camt_rpc2` | `float` | `float` |  |  | CC: Sales Contract Line Called Amount in Reporting Currency 2 |
| `camt_rfrc` | `float` | `float` |  |  | CC: Sales Contract Line Called Amount in Reference Currency |
| `camt_dtwc` | `float` | `float` |  |  | CC: Sales Contract Line Called Amount in Data Warehouse Currency |
| `gamt_lclc` | `float` | `float` |  |  | CC: Sales Contract Line Gross Amount in Local Currency |
| `gamt_rpc1` | `float` | `float` |  |  | CC: Sales Contract Line Gross Amount in Reporting Currency 1 |
| `gamt_rpc2` | `float` | `float` |  |  | CC: Sales Contract Line Gross Amount in Reporting Currency 2 |
| `gamt_rfrc` | `float` | `float` |  |  | CC: Sales Contract Line Gross Amount in Reference Currency |
| `gamt_dtwc` | `float` | `float` |  |  | CC: Sales Contract Line Gross Amount in Data Warehouse Currency |
| `gamt_trnc` | `float` | `float` |  |  | CC: Sales Contract Line Gross Amount in Transaction Currency |
| `namt_lclc` | `float` | `float` |  |  | CC: Sales Contract Line Net Amount in Local Currency |
| `namt_rpc1` | `float` | `float` |  |  | CC: Sales Contract Line Net Amount in Reporting Currency 1 |
| `namt_rpc2` | `float` | `float` |  |  | CC: Sales Contract Line Net Amount in Reporting Currency 2 |
| `namt_rfrc` | `float` | `float` |  |  | CC: Sales Contract Line Net Amount in Reference Currency |
| `namt_dtwc` | `float` | `float` |  |  | CC: Sales Contract Line Net Amount in Data Warehouse Currency |
| `namt_trnc` | `float` | `float` |  |  | CC: Sales Contract Line Net Amount in Transaction Currency |
| `bamt_lclc` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Amount in Local Currency |
| `bamt_rpc1` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Amount in Reporting Currency 1 |
| `bamt_rpc2` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Amount in Reporting Currency 2 |
| `bamt_rfrc` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Amount in Reference Currency |
| `bamt_dtwc` | `float` | `float` |  |  | CC: Sales Contract Line Invoiced Amount in Data Warehouse Currency |
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

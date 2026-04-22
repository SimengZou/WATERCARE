# tdsls307

LN tdsls307 Sales Schedule Lines table, Generated 2026-04-10T19:41:25Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsls307` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `schn`, `sctp`, `revn`, `spon` |
| **Column count** | 202 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `schn` | `string` | `varchar` | `PK` | `not_null` | (required) Schedule. Max length: 9 |
| `sctp` | `integer` | `int` | `PK` | `not_null` | (required) Schedule Type. Allowed values: 1, 2, 3, 4 |
| `sctp_kw` | `string` | `varchar` |  |  | Schedule Type (keyword). Allowed values: tdsls.reltype.matrelease, tdsls.reltype.shipschedule, tdsls.reltype.seqschedule, tdsls.reltype.pickupsheet |
| `revn` | `integer` | `int` | `PK` | `not_null` | (required) Schedule Revision |
| `spon` | `integer` | `int` | `PK` | `not_null` | (required) Schedule Position |
| `qoor` | `float` | `float` |  |  | Quantity |
| `qoad` | `float` | `float` |  |  | Adjustment Quantity |
| `cuqs` | `string` | `varchar` |  |  | Sales Unit. Max length: 3 |
| `cvqs` | `float` | `float` |  |  | Sales Unit Conversion Factor |
| `qrrq` | `float` | `float` |  |  | Required Quantity |
| `curq` | `string` | `varchar` |  |  | Required Quantity Unit. Max length: 3 |
| `cvrq` | `float` | `float` |  |  | Required Quantity Unit Conversion Factor |
| `qccf` | `float` | `float` |  |  | Confirmed Quantity |
| `cucf` | `string` | `varchar` |  |  | Confirmed Quantity Unit. Max length: 3 |
| `cvcf` | `float` | `float` |  |  | Confirmed Quantity Unit Conversion Factor |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `cfdt` | `timestamp` | `timestamp_ntz` |  |  | Confirmation Date |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `tiun` | `integer` | `int` |  |  | Time Unit. Allowed values: 1, 2, 3, 4, 5, 6 |
| `tiun_kw` | `string` | `varchar` |  |  | Time Unit (keyword). Allowed values: tdsls.tiun.hour, tdsls.tiun.day, tdsls.tiun.week, tdsls.tiun.month, tdsls.tiun.four.week, tdsls.tiun.not.applicable |
| `ctyp` | `integer` | `int` |  |  | Customer Requirement Type. Allowed values: 1, 2, 3, 4 |
| `ctyp_kw` | `string` | `varchar` |  |  | Customer Requirement Type (keyword). Allowed values: tdpur.rtyp.immediate, tdpur.rtyp.firm, tdpur.rtyp.planned, tdpur.rtyp.backorder |
| `citr__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item Revision - base language. Max length: 35 |
| `rtyp` | `integer` | `int` |  |  | Requirement Type. Allowed values: 1, 2, 3, 4 |
| `rtyp_kw` | `string` | `varchar` |  |  | Requirement Type (keyword). Allowed values: tdpur.rtyp.immediate, tdpur.rtyp.firm, tdpur.rtyp.planned, tdpur.rtyp.backorder |
| `qicq` | `float` | `float` |  |  | Customer Required Quantity |
| `dmor` | `integer` | `int` |  |  | Demand Origin. Allowed values: 10, 20, 30, 40 |
| `dmor_kw` | `string` | `varchar` |  |  | Demand Origin (keyword). Allowed values: tdsls.dmor.not.applicable, tdsls.dmor.customer, tdsls.dmor.manual, tdsls.dmor.shipment |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stsi` | `string` | `varchar` |  |  | Ship-to Site. Max length: 9 |
| `stwh` | `string` | `varchar` |  |  | Ship-to Warehouse. Max length: 6 |
| `imcs` | `string` | `varchar` |  |  | Intermediate Consignee. Max length: 35 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `revi` | `string` | `varchar` |  |  | Engineering Item Rev.. Max length: 6 |
| `leng` | `float` | `float` |  |  | Length |
| `thic` | `float` | `float` |  |  | Height |
| `widt` | `float` | `float` |  |  | Width |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdsls.stat.created, tdsls.stat.adjusted, tdsls.stat.approved, tdsls.stat.order.generated, tdsls.stat.partial.deliver, tdsls.stat.goods.delivered, tdsls.stat.released.to.inv, tdsls.stat.invoiced, tdsls.stat.processed, tdsls.stat.cancelled, tdsls.stat.cancel.in.proc, tdsls.stat.replaced, tdsls.stat.replace.in.proc |
| `bkyn` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `bkyn_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ddta` | `timestamp` | `timestamp_ntz` |  |  | Last Delivery Date |
| `dlno` | `string` | `varchar` |  |  | Last Shipment. Max length: 9 |
| `qidl` | `float` | `float` |  |  | Delivered Quantity |
| `cuva` | `float` | `float` |  |  | Customs Value |
| `pmnt` | `integer` | `int` |  |  | Payment. Allowed values: 10, 20, 30, 90 |
| `pmnt_kw` | `string` | `varchar` |  |  | Payment (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `sbim` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbim_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Last Invoice Date |
| `scmp` | `integer` | `int` |  |  | Last Invoice Company |
| `ttyp` | `string` | `varchar` |  |  | Last Transaction Type. Max length: 3 |
| `invn` | `integer` | `int` |  |  | Last Invoice |
| `chan` | `string` | `varchar` |  |  | Channels. Max length: 3 |
| `dest` | `integer` | `int` |  |  | Goods Flow. Allowed values: 1, 2, 3, 4 |
| `dest_kw` | `string` | `varchar` |  |  | Goods Flow (keyword). Allowed values: tdsls.dest.warehousing, tdsls.dest.sales, tdsls.dest.transfer, tdsls.dest.direct.delivery |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `opon` | `integer` | `int` |  |  | Line |
| `seqn` | `integer` | `int` |  |  | Sequence |
| `rrdt` | `timestamp` | `timestamp_ntz` |  |  | Required Planned Receipt Date |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Confirmed Planned Receipt Date |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `refs__en_us` | `string` | `varchar` |  | `not_null` | (required) Shipment Reference - base language. Max length: 35 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `rcod` | `string` | `varchar` |  |  | Exempt Reason. Max length: 6 |
| `ceno` | `string` | `varchar` |  |  | Exempt Certificate. Max length: 24 |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `bptc` | `string` | `varchar` |  |  | Business Partner Tax Country. Max length: 3 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `cchn` | `string` | `varchar` |  |  | Customer Schedule Number. Max length: 30 |
| `cchp` | `string` | `varchar` |  |  | Customer Schedule Position. Max length: 14 |
| `prid` | `string` | `varchar` |  |  | Price ID. Max length: 22 |
| `samt` | `float` | `float` |  |  | Schedule Amount |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Contract Position |
| `ccof` | `string` | `varchar` |  |  | Contract Office. Max length: 6 |
| `hold` | `integer` | `int` |  |  | Put on Hold. Allowed values: 1, 2 |
| `hold_kw` | `string` | `varchar` |  |  | Put on Hold (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `smsl` | `integer` | `int` |  |  | Subcontracting Supply Line. Allowed values: 0, 1, 2 |
| `smsl_kw` | `string` | `varchar` |  |  | Subcontracting Supply Line (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 30 |
| `prfa__en_us` | `string` | `varchar` |  | `not_null` | (required) Packaging Reference A - base language. Max length: 35 |
| `prfb__en_us` | `string` | `varchar` |  | `not_null` | (required) Packaging Reference B - base language. Max length: 35 |
| `dock` | `string` | `varchar` |  |  | Dock Location. Max length: 30 |
| `dlpt` | `string` | `varchar` |  |  | Delivery Point. Max length: 9 |
| `styp` | `string` | `varchar` |  |  | Sales Type. Max length: 6 |
| `lnor` | `integer` | `int` |  |  | Schedule Line Origin. Allowed values: 5, 10, 15, 20, 25, 90 |
| `lnor_kw` | `string` | `varchar` |  |  | Schedule Line Origin (keyword). Allowed values: tdsls.lnor.sales.release, tdsls.lnor.sales.schedule, tdsls.lnor.planning, tdsls.lnor.purchase, tdsls.lnor.manual, tdsls.lnor.not.applicable |
| `casi` | `string` | `varchar` |  |  | Additional Statistical Information Set. Max length: 3 |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `pric` | `float` | `float` |  |  | Obsolete |
| `cups` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cvps` | `float` | `float` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Schedule Line Text |
| `schn_sctp_revn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls311 Sales Schedules |
| `cuqs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `curq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cucf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_dlpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom134 Delivery Points |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `stwh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `item_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa001 Item - Sales |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `chan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs066 Channels |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `cono_pono_ccof_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls301 Sales Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls300 Sales Contracts |
| `ccof_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `styp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs202 Sales Types |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `gamt_lclc` | `float` | `float` |  |  | CC: Sales Schedule Line Gross Amount in Local Currency |
| `gamt_rpc1` | `float` | `float` |  |  | CC: Sales Schedule Line Gross Amount in Reporting Currency 1 |
| `gamt_rpc2` | `float` | `float` |  |  | CC: Sales Schedule Line Gross Amount in Reporting Currency 2 |
| `gamt_rfrc` | `float` | `float` |  |  | CC: Sales Schedule Line Gross Amount in Reference Currency |
| `gamt_dtwc` | `float` | `float` |  |  | CC: Sales Schedule Line Gross Amount in Data Warehouse Currency |
| `gamt_trnc` | `float` | `float` |  |  | CC: Sales Schedule Line Gross Amount in Transaction Currency |
| `namt_lclc` | `float` | `float` |  |  | CC: Sales Schedule Line Net Amount in Local Currency |
| `namt_rpc1` | `float` | `float` |  |  | CC: Sales Schedule Line Net Amount in Reporting Currency 1 |
| `namt_rpc2` | `float` | `float` |  |  | CC: Sales Schedule Line Net Amount in Reporting Currency 2 |
| `namt_rfrc` | `float` | `float` |  |  | CC: Sales Schedule Line Net Amount in Reference Currency |
| `namt_dtwc` | `float` | `float` |  |  | CC: Sales Schedule Line Net Amount in Data Warehouse Currency |
| `namt_trnc` | `float` | `float` |  |  | CC: Sales Schedule Line Net Amount in Transaction Currency |
| `tcpr_trnc` | `float` | `float` |  |  | CC: Sales Schedule Line Total Cost Price in Transaction Currency |
| `tcpr_lclc` | `float` | `float` |  |  | CC: Sales Schedule Line Total Cost Price in Local Currency |
| `tcpr_rpc1` | `float` | `float` |  |  | CC: Sales Schedule Line Total Cost Price in Reporting Currency 1 |
| `tcpr_rpc2` | `float` | `float` |  |  | CC: Sales Schedule Line Total Cost Price in Reporting Currency 2 |
| `tcpr_rfrc` | `float` | `float` |  |  | CC: Sales Schedule Line Total Cost Price in Reference Currency |
| `tcpr_dtwc` | `float` | `float` |  |  | CC: Sales Schedule Line Total Cost Price in Data Warehouse Currency |
| `qoor_invu` | `float` | `float` |  |  | CC: Sales Schedule Line Quantity in Inventory Unit |
| `qoor_buar` | `float` | `float` |  |  | CC: Sales Schedule Line Quantity in Base Unit Area |
| `qoor_buln` | `float` | `float` |  |  | CC: Sales Schedule Line Quantity in Base Unit Length |
| `qoor_bupc` | `float` | `float` |  |  | CC: Sales Schedule Line Quantity in Base Unit Piece |
| `qoor_butm` | `float` | `float` |  |  | CC: Sales Schedule Line Quantity in Base Unit Time |
| `qoor_buvl` | `float` | `float` |  |  | CC: Sales Schedule Line Quantity in Base Unit Volume |
| `qoor_buwg` | `float` | `float` |  |  | CC: Sales Schedule Line Quantity in Base Unit Weight |
| `qoad_invu` | `float` | `float` |  |  | CC: Sales Schedule Line Adjustment Quantity in Inventory Unit |
| `qoad_buar` | `float` | `float` |  |  | CC: Sales Schedule Line Adjustment Quantity in Base Unit Area |
| `qoad_buln` | `float` | `float` |  |  | CC: Sales Schedule Line Adjustment Quantity in Base Unit Length |
| `qoad_bupc` | `float` | `float` |  |  | CC: Sales Schedule Line Adjustment Quantity in Base Unit Piece |
| `qoad_butm` | `float` | `float` |  |  | CC: Sales Schedule Line Adjustment Quantity in Base Unit Time |
| `qoad_buvl` | `float` | `float` |  |  | CC: Sales Schedule Line Adjustment Quantity in Base Unit Volume |
| `qoad_buwg` | `float` | `float` |  |  | CC: Sales Schedule Line Adjustment Quantity in Base Unit Weight |
| `qrrq_trnu` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Transaction Unit |
| `qrrq_invu` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Inventory Unit |
| `qrrq_buar` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Base Unit Area |
| `qrrq_buln` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Base Unit Length |
| `qrrq_bupc` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Base Unit Piece |
| `qrrq_butm` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Base Unit Time |
| `qrrq_buvl` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Base Unit Volume |
| `qrrq_buwg` | `float` | `float` |  |  | CC: Sales Schedule Line Required Quantity in Base Unit Weight |
| `qccf_trnu` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Transaction Unit |
| `qccf_invu` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Inventory Unit |
| `qccf_buar` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Base Unit Area |
| `qccf_buln` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Base Unit Length |
| `qccf_bupc` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Base Unit Piece |
| `qccf_butm` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Base Unit Time |
| `qccf_buvl` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Base Unit Volume |
| `qccf_buwg` | `float` | `float` |  |  | CC: Sales Schedule Line Confirmed Quantity in Base Unit Weight |
| `qidl_trnu` | `float` | `float` |  |  | CC: Sales Schedule Line Delivered Quantity in Transaction Unit |
| `qidl_buar` | `float` | `float` |  |  | CC: Sales Schedule Line Delivered Quantity in Base Unit Area |
| `qidl_buln` | `float` | `float` |  |  | CC: Sales Schedule Line Delivered Quantity in Base Unit Length |
| `qidl_bupc` | `float` | `float` |  |  | CC: Sales Schedule Line Delivered Quantity in Base Unit Piece |
| `qidl_butm` | `float` | `float` |  |  | CC: Sales Schedule Line Delivered Quantity in Base Unit Time |
| `qidl_buvl` | `float` | `float` |  |  | CC: Sales Schedule Line Delivered Quantity in Base Unit Volume |
| `qidl_buwg` | `float` | `float` |  |  | CC: Sales Schedule Line Delivered Quantity in Base Unit Weight |
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

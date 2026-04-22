# tdsls311

LN tdsls311 Sales Schedules table, Generated 2026-04-10T19:41:25Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsls311` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `schn`, `sctp`, `revn` |
| **Column count** | 162 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `schn` | `string` | `varchar` | `PK` | `not_null` | (required) Schedule. Max length: 9 |
| `sctp` | `integer` | `int` | `PK` | `not_null` | (required) Schedule Type. Allowed values: 1, 2, 3, 4 |
| `sctp_kw` | `string` | `varchar` |  |  | Schedule Type (keyword). Allowed values: tdsls.reltype.matrelease, tdsls.reltype.shipschedule, tdsls.reltype.seqschedule, tdsls.reltype.pickupsheet |
| `revn` | `integer` | `int` | `PK` | `not_null` | (required) Schedule Revision |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Sold-to Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Sold-to Contact. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Area. Max length: 3 |
| `citt` | `string` | `varchar` |  |  | Item Code System. Max length: 3 |
| `citm__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item - base language. Max length: 35 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Position |
| `ccof` | `string` | `varchar` |  |  | Contract Office. Max length: 6 |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 30 |
| `ccrf__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Contract Reference - base language. Max length: 30 |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `fdpt` | `string` | `varchar` |  |  | Financial Department. Max length: 6 |
| `gdat` | `timestamp` | `timestamp_ntz` |  |  | Generation Date |
| `sllr` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `osrp` | `string` | `varchar` |  |  | External Sales Representative. Max length: 9 |
| `plan` | `string` | `varchar` |  |  | Planner. Max length: 9 |
| `corg` | `integer` | `int` |  |  | Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 21, 22, 25, 30, 35, 40, 45, 99 |
| `corg_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tdsls.corg.contracts, tdsls.corg.quotations, tdsls.corg.eop, tdsls.corg.manual, tdsls.corg.phone, tdsls.corg.fax, tdsls.corg.mail, tdsls.corg.opportunity, tdsls.corg.crm, tdsls.corg.consumption, tdsls.corg.quicklist, tdsls.corg.service, tdsls.corg.edi.dir.del, tdsls.corg.retrobill, tdsls.corg.planning, tdsls.corg.purchase, tdsls.corg.shipment, tdsls.corg.price.calc, tdsls.corg.not.appl |
| `dmor` | `integer` | `int` |  |  | Demand Origin. Allowed values: 10, 20, 30, 40 |
| `dmor_kw` | `string` | `varchar` |  |  | Demand Origin (keyword). Allowed values: tdsls.dmor.not.applicable, tdsls.dmor.customer, tdsls.dmor.manual, tdsls.dmor.shipment |
| `dest` | `integer` | `int` |  |  | Goods Flow. Allowed values: 1, 2, 3, 4 |
| `dest_kw` | `string` | `varchar` |  |  | Goods Flow (keyword). Allowed values: tdsls.dest.warehousing, tdsls.dest.sales, tdsls.dest.transfer, tdsls.dest.direct.delivery |
| `fcrt` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rats_1` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_2` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_3` | `float` | `float` |  |  | Currency Rate Sales |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `ratt` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `lsel` | `integer` | `int` |  |  | Lot Selection. Allowed values: 1, 2, 3 |
| `lsel_kw` | `string` | `varchar` |  |  | Lot Selection (keyword). Allowed values: tclsel.any, tclsel.same, tclsel.specific |
| `motv` | `string` | `varchar` |  |  | Motive of Transport. Max length: 6 |
| `delc` | `string` | `varchar` |  |  | Delivery Code. Max length: 6 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `sbim` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbim_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rsiu` | `integer` | `int` |  |  | Reference Schedule in use. Allowed values: 1, 2 |
| `rsiu_kw` | `string` | `varchar` |  |  | Reference Schedule in use (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rviu` | `integer` | `int` |  |  | Revisions in Use. Allowed values: 1, 2 |
| `rviu_kw` | `string` | `varchar` |  |  | Revisions in Use (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ucos` | `integer` | `int` |  |  | Use Customer Order for Schedules. Allowed values: 1, 2 |
| `ucos_kw` | `string` | `varchar` |  |  | Use Customer Order for Schedules (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ucms` | `integer` | `int` |  |  | Use Extended Confirmation Logic for Schedules. Allowed values: 1, 2 |
| `ucms_kw` | `string` | `varchar` |  |  | Use Extended Confirmation Logic for Schedules (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ltcs` | `integer` | `int` |  |  | Linked to Pick-up Sheets. Allowed values: 1, 2 |
| `ltcs_kw` | `string` | `varchar` |  |  | Linked to Pick-up Sheets (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `excp` | `integer` | `int` |  |  | Exceptions Present. Allowed values: 1, 2 |
| `excp_kw` | `string` | `varchar` |  |  | Exceptions Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `odis` | `float` | `float` |  |  | Order Discount |
| `qifa` | `float` | `float` |  |  | FAB Authorization |
| `qira` | `float` | `float` |  |  | RAW Authorization |
| `qifh` | `float` | `float` |  |  | High FAB Authorization |
| `qirh` | `float` | `float` |  |  | High RAW Authorization |
| `qicr` | `float` | `float` |  |  | Received CUM |
| `qicp` | `float` | `float` |  |  | Prior Required CUM |
| `qics` | `float` | `float` |  |  | Cumulative Shipped Quantity |
| `qici` | `float` | `float` |  |  | Invoiced CUM |
| `csdt` | `timestamp` | `timestamp_ntz` |  |  | Customer CUMs Reset Date |
| `edfb` | `timestamp` | `timestamp_ntz` |  |  | FAB Authorization Through Date |
| `edrw` | `timestamp` | `timestamp_ntz` |  |  | RAW Authorization Through Date |
| `qilr` | `float` | `float` |  |  | Last Receipt Quantity |
| `lrdt` | `timestamp` | `timestamp_ntz` |  |  | Last Receipt Date |
| `lasn` | `string` | `varchar` |  |  | Last ASN. Max length: 16 |
| `lsid` | `string` | `varchar` |  |  | Last Shipment. Max length: 9 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdsls.hstat.created, tdsls.hstat.adjusted, tdsls.hstat.approved, tdsls.hstat.terminated, tdsls.hstat.terminate.on, tdsls.hstat.position.full, tdsls.hstat.processed, tdsls.hstat.processing.on, tdsls.hstat.replaced, tdsls.hstat.replacing.on |
| `bkyn` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `bkyn_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `osch` | `string` | `varchar` |  |  | Old Schedule. Max length: 9 |
| `strc` | `string` | `varchar` |  |  | Reason For Termination. Max length: 6 |
| `sodb` | `integer` | `int` |  |  | Shipment / Receipt Based. Allowed values: 1, 2 |
| `sodb_kw` | `string` | `varchar` |  |  | Shipment / Receipt Based (keyword). Allowed values: tdipu.sodb.shipment, tdipu.sodb.delivery |
| `hold` | `integer` | `int` |  |  | Put on Hold. Allowed values: 1, 2 |
| `hold_kw` | `string` | `varchar` |  |  | Put on Hold (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hiss` | `integer` | `int` |  |  | Log Schedule History. Allowed values: 1, 2 |
| `hiss_kw` | `string` | `varchar` |  |  | Log Schedule History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cons` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cons_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aitc__en_us` | `string` | `varchar` |  | `not_null` | (required) Obsolete - base language. Max length: 35 |
| `revi` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `txta` | `integer` | `int` |  |  | Schedule Line Text |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `item_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa001 Item - Sales |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `cono_pono_ccof_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls301 Sales Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls300 Sales Contracts |
| `ccof_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `fdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `sllr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `osrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `plan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ratt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `motv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `delc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `strc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

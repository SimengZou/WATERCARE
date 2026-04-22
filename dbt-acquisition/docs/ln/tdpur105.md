# tdpur105

LN tdpur105 RFQ Bidders table, Generated 2026-04-10T19:41:18Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur105` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `qono`, `otbp` |
| **Column count** | 104 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `qono` | `string` | `varchar` | `PK` | `not_null` | (required) RFQ. Max length: 9 |
| `otbp` | `string` | `varchar` | `PK` | `not_null` | (required) Bidder. Max length: 9 |
| `eint` | `integer` | `int` |  |  | External Integration. Allowed values: 1, 2 |
| `eint_kw` | `string` | `varchar` |  |  | External Integration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qspa` | `integer` | `int` |  |  | Status. Allowed values: 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60 |
| `qspa_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdpur.rfqs.created, tdpur.rfqs.pending.resp, tdpur.rfqs.sent, tdpur.rfqs.modified, tdpur.rfqs.responded, tdpur.rfqs.in.process, tdpur.rfqs.no.bid, tdpur.rfqs.no.response, tdpur.rfqs.negotiating, tdpur.rfqs.accepted, tdpur.rfqs.rejected, tdpur.rfqs.processed, tdpur.rfqs.cancelled |
| `otad` | `string` | `varchar` |  |  | Buy-from Address. Max length: 9 |
| `otcn` | `string` | `varchar` |  |  | Buy-from Contact. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfcn` | `string` | `varchar` |  |  | Ship-from Contact. Max length: 9 |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from BP. Max length: 9 |
| `ifad` | `string` | `varchar` |  |  | Invoice-from Address. Max length: 9 |
| `ifcn` | `string` | `varchar` |  |  | Invoice-from Contact. Max length: 9 |
| `ptbp` | `string` | `varchar` |  |  | Pay-to Business Partner. Max length: 9 |
| `ptad` | `string` | `varchar` |  |  | Pay-to Address. Max length: 9 |
| `ptcn` | `string` | `varchar` |  |  | Pay-to Contact. Max length: 9 |
| `rtdt` | `timestamp` | `timestamp_ntz` |  |  | Response Date |
| `rptd` | `timestamp` | `timestamp_ntz` |  |  | Returned On |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `send` | `integer` | `int` |  |  | Send RFQ. Allowed values: 1, 2 |
| `send_kw` | `string` | `varchar` |  |  | Send RFQ (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rsts` | `integer` | `int` |  |  | Reminder Status. Allowed values: 1, 2, 3 |
| `rsts_kw` | `string` | `varchar` |  |  | Reminder Status (keyword). Allowed values: tcrsts.remind, tcrsts.delete, tcrsts.none |
| `rnop` | `integer` | `int` |  |  | Reminder Number of Periods |
| `rpru` | `integer` | `int` |  |  | Reminder Period Unit. Allowed values: 5, 10, 15 |
| `rpru_kw` | `string` | `varchar` |  |  | Reminder Period Unit (keyword). Allowed values: tdgen.rpru.hours, tdgen.rpru.days, tdgen.rpru.weeks |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `fixr` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fixr_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `ratp_1` | `float` | `float` |  |  | Purchase Rate |
| `ratp_2` | `float` | `float` |  |  | Purchase Rate |
| `ratp_3` | `float` | `float` |  |  | Purchase Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `tyfb` | `integer` | `int` |  |  | Thank You for Bidding Letter. Allowed values: 1, 2 |
| `tyfb_kw` | `string` | `varchar` |  |  | Thank You for Bidding Letter (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rqno__en_us` | `string` | `varchar` |  | `not_null` | (required) Supplier Quotation - base language. Max length: 30 |
| `cplp` | `string` | `varchar` |  |  | Purchase Price List. Max length: 3 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Area. Max length: 3 |
| `odis` | `float` | `float` |  |  | Order Discount |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `ratt` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `iebp` | `integer` | `int` |  |  | Invoice External Business Partner. Allowed values: 1, 2 |
| `iebp_kw` | `string` | `varchar` |  |  | Invoice External Business Partner (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iafc` | `integer` | `int` |  |  | Invoice Freight Costs Based On. Allowed values: 5, 10, 15, 20 |
| `iafc_kw` | `string` | `varchar` |  |  | Invoice Freight Costs Based On (keyword). Allowed values: tccom.infr.estimate, tccom.infr.actual, tccom.infr.costplus, tccom.infr.not.applic |
| `lccl` | `string` | `varchar` |  |  | Landed Costs Classification. Max length: 12 |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `etpc` | `integer` | `int` |  |  | Exclude from Target Price Calculation. Allowed values: 1, 2 |
| `etpc_kw` | `string` | `varchar` |  |  | Exclude from Target Price Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `site` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `qono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur100 Requests for Quotation |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `otad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `otcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ifad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ifcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ptad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ptcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `cplp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `ratt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `lccl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tclct020 Landed Costs Classifications |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
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

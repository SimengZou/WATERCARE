# tdpur100

LN tdpur100 Requests for Quotation table, Generated 2026-04-10T19:41:18Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `qono` |
| **Column count** | 68 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `qono` | `string` | `varchar` | `PK` | `not_null` | (required) RFQ. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `rfqt` | `string` | `varchar` |  |  | RFQ Type. Max length: 9 |
| `negt` | `integer` | `int` |  |  | Negotiation Type. Allowed values: 5, 10, 15 |
| `negt_kw` | `string` | `varchar` |  |  | Negotiation Type (keyword). Allowed values: tcrfq.neg.type.traditional, tcrfq.neg.type.sealed.bid, tcrfq.neg.type.reverse.auction |
| `cneg` | `integer` | `int` |  |  | Contract Negotiation. Allowed values: 1, 2 |
| `cneg_kw` | `string` | `varchar` |  |  | Contract Negotiation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `qdat` | `timestamp` | `timestamp_ntz` |  |  | RFQ Date |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `qspa` | `integer` | `int` |  |  | Status. Allowed values: 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60 |
| `qspa_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdpur.rfqs.created, tdpur.rfqs.pending.resp, tdpur.rfqs.sent, tdpur.rfqs.modified, tdpur.rfqs.responded, tdpur.rfqs.in.process, tdpur.rfqs.no.bid, tdpur.rfqs.no.response, tdpur.rfqs.negotiating, tdpur.rfqs.accepted, tdpur.rfqs.rejected, tdpur.rfqs.processed, tdpur.rfqs.cancelled |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `site` | `string` | `varchar` |  |  | Receiving Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Receipt Address. Max length: 9 |
| `refa__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference A - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference B - base language. Max length: 30 |
| `rtdt` | `timestamp` | `timestamp_ntz` |  |  | Response Date |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `ddat` | `timestamp` | `timestamp_ntz` |  |  | Receipt Date |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Start Date for Receipt Period |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | End Date for Receipt Period |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Reminder Date |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `cset` | `string` | `varchar` |  |  | Criteria Set. Max length: 6 |
| `ulcr` | `integer` | `int` |  |  | Apply Landed Costs. Allowed values: 1, 2 |
| `ulcr_kw` | `string` | `varchar` |  |  | Apply Landed Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uptc` | `integer` | `int` |  |  | Use Payment Terms for Comparison. Allowed values: 1, 2 |
| `uptc_kw` | `string` | `varchar` |  |  | Use Payment Terms for Comparison (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `enlh` | `integer` | `int` |  |  | Enhanced Line Handling. Allowed values: 1, 2 |
| `enlh_kw` | `string` | `varchar` |  |  | Enhanced Line Handling (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `etpc` | `integer` | `int` |  |  | Exclude from Target Price Calculation. Allowed values: 1, 2 |
| `etpc_kw` | `string` | `varchar` |  |  | Exclude from Target Price Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tpbk` | `string` | `varchar` |  |  | Target Price Book. Max length: 9 |
| `cotp` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Header Text |
| `txtb` | `integer` | `int` |  |  | Footer Text |
| `rfqt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur096 RFQ Types |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `cset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur190 RFQ Criterion Sets |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `tpbk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg013 Target Price Books |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

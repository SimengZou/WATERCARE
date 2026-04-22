# tdsls300

LN tdsls300 Sales Contracts table, Generated 2026-04-10T19:41:24Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsls300` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono` |
| **Column count** | 95 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cdes__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `adco` | `integer` | `int` |  |  | Action on Deviating Customer Order. Allowed values: 5, 10, 15, 20 |
| `adco_kw` | `string` | `varchar` |  |  | Action on Deviating Customer Order (keyword). Allowed values: tdsls.adco.not.applicable, tdsls.adco.block, tdsls.adco.cont.release, tdsls.adco.cont.contract |
| `aeco` | `integer` | `int` |  |  | Action on Deviating Empty Customer Order. Allowed values: 5, 10, 15, 20 |
| `aeco_kw` | `string` | `varchar` |  |  | Action on Deviating Empty Customer Order (keyword). Allowed values: tdsls.adco.not.applicable, tdsls.adco.block, tdsls.adco.cont.release, tdsls.adco.cont.contract |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 30 |
| `ccrf__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Contract Reference - base language. Max length: 30 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference A - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference B - base language. Max length: 30 |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Contract Date |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `icyp` | `integer` | `int` |  |  | Contract Type. Allowed values: 1, 2 |
| `icyp_kw` | `string` | `varchar` |  |  | Contract Type (keyword). Allowed values: tcicyp.year, tcicyp.proj |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Sold-to Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Sold-to Contact. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `itcn` | `string` | `varchar` |  |  | Invoice-to Contact. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `pfcn` | `string` | `varchar` |  |  | Pay-by Contact. Max length: 9 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `crep` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `osrp` | `string` | `varchar` |  |  | External Sales Representative. Max length: 9 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `icap` | `integer` | `int` |  |  | Contract Status. Allowed values: 1, 2, 3 |
| `icap_kw` | `string` | `varchar` |  |  | Contract Status (keyword). Allowed values: tcicap.free, tcicap.active, tcicap.closed |
| `icpr` | `integer` | `int` |  |  | Contract Acknowledgement. Allowed values: 1, 2 |
| `icpr_kw` | `string` | `varchar` |  |  | Contract Acknowledgement (keyword). Allowed values: tcicpr.print, tcicpr.dont.print |
| `csts` | `integer` | `int` |  |  | Print Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `csts_kw` | `string` | `varchar` |  |  | Print Status (keyword). Allowed values: tccsts.printed, tccsts.not.printed, tccsts.changed, tccsts.draft, tccsts.original, tccsts.changes.printed |
| `hval` | `integer` | `int` |  |  | tdgen.obsolete. Allowed values: 1, 2 |
| `hval_kw` | `string` | `varchar` |  |  | tdgen.obsolete (keyword). Allowed values: tdpur.hval.hold, tdpur.hval.continue |
| `ocon` | `string` | `varchar` |  |  | Original Contract. Max length: 9 |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `rofc` | `string` | `varchar` |  |  | Responsible Office. Max length: 6 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `sotp` | `string` | `varchar` |  |  | Sales Order Type. Max length: 3 |
| `trid` | `string` | `varchar` |  |  | Terms and Conditions ID. Max length: 9 |
| `vdco` | `integer` | `int` |  |  | Validate Contract. Allowed values: 1, 2 |
| `vdco_kw` | `string` | `varchar` |  |  | Validate Contract (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Header Text |
| `txtb` | `integer` | `int` |  |  | Footer Text |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `osrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `ocon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls300 Sales Contracts |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `rofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `sotp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls094 Sales Order Types |
| `trid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctrm100 Terms and Conditions |
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

# tsctm200

LN tsctm200 Contract Quotes table, Generated 2026-04-10T19:42:33Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ccqu` |
| **Column count** | 150 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ccqu` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Quote. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `sear__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Argument - base language. Max length: 16 |
| `ctpc` | `string` | `varchar` |  |  | Contract Type. Max length: 3 |
| `qudt` | `date` | `date` |  |  | Quote Date |
| `qxdt` | `date` | `date` |  |  | Expiry Date |
| `squo` | `integer` | `int` |  |  | Quote Status. Allowed values: 5, 10, 15, 20, 25 |
| `squo_kw` | `string` | `varchar` |  |  | Quote Status (keyword). Allowed values: tsctm.squo.free, tsctm.squo.printed, tsctm.squo.accepted, tsctm.squo.processed, tsctm.squo.canceled |
| `cwoc` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `emno` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `supe` | `float` | `float` |  |  | Probability Percentage |
| `cdis` | `string` | `varchar` |  |  | Success/ Failure. Max length: 6 |
| `cplt` | `string` | `varchar` |  |  | Sales Price List. Max length: 3 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Contact Person. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `refa__en_us` | `string` | `varchar` |  | `not_null` | (required) First Invoice Reference - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Second Invoice Reference - base language. Max length: 30 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `csco` | `string` | `varchar` |  |  | Original Service Contract. Max length: 9 |
| `psrc` | `string` | `varchar` |  |  | Processed Service Contract. Max length: 9 |
| `term` | `integer` | `int` |  |  | Quote Terms |
| `cedt` | `date` | `date` |  |  | Effective Date of Current Contract |
| `nrpe` | `integer` | `int` |  |  | No of Periods of Quote Duration |
| `peru` | `integer` | `int` |  |  | Period Unit of Quote Duration. Allowed values: 5, 10, 15, 20, 25 |
| `peru_kw` | `string` | `varchar` |  |  | Period Unit of Quote Duration (keyword). Allowed values: tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `prmt` | `integer` | `int` |  |  | Pricing Method. Allowed values: 5, 10, 15, 20 |
| `prmt_kw` | `string` | `varchar` |  |  | Pricing Method (keyword). Allowed values: tsctm.prmt.sales, tsctm.prmt.budget, tsctm.prmt.price, tsctm.prmt.template.price |
| `perc` | `float` | `float` |  |  | Percentage of Sales Value |
| `ovyn` | `integer` | `int` |  |  | Overtime Allowed. Allowed values: 1, 2 |
| `ovyn_kw` | `string` | `varchar` |  |  | Overtime Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cryn` | `integer` | `int` |  |  | Contract Renewal. Allowed values: 1, 2 |
| `cryn_kw` | `string` | `varchar` |  |  | Contract Renewal (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crnp` | `integer` | `int` |  |  | Renewal Period |
| `crpu` | `integer` | `int` |  |  | Period Unit. Allowed values: 5, 10, 15, 20, 25 |
| `crpu_kw` | `string` | `varchar` |  |  | Period Unit (keyword). Allowed values: tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `indx` | `integer` | `int` |  |  | Price Indexation. Allowed values: 1, 2 |
| `indx_kw` | `string` | `varchar` |  |  | Price Indexation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `indt` | `date` | `date` |  |  | Indexation Start Date |
| `innp` | `integer` | `int` |  |  | Indexation Interval |
| `inpu` | `integer` | `int` |  |  | Period Unit. Allowed values: 5, 10, 15, 20, 25 |
| `inpu_kw` | `string` | `varchar` |  |  | Period Unit (keyword). Allowed values: tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `cind` | `string` | `varchar` |  |  | Indexation Template. Max length: 3 |
| `inch` | `integer` | `int` |  |  | Incidental Changes. Allowed values: 1, 2 |
| `inch_kw` | `string` | `varchar` |  |  | Incidental Changes (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `icpn` | `float` | `float` |  |  | Penalty |
| `ditc` | `integer` | `int` |  |  | Define Installment Template per Configuration. Allowed values: 1, 2 |
| `ditc_kw` | `string` | `varchar` |  |  | Define Installment Template per Configuration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ctin` | `string` | `varchar` |  |  | Installment Template. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ratc_1` | `float` | `float` |  |  | Currency Rate |
| `ratc_2` | `float` | `float` |  |  | Currency Rate |
| `ratc_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rtdt` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Rate Type. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `ccty` | `string` | `varchar` |  |  | Country. Max length: 3 |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `itad` | `string` | `varchar` |  |  | Invoice-to Address. Max length: 9 |
| `itcn` | `string` | `varchar` |  |  | Invoice-to Contact. Max length: 9 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `pfad` | `string` | `varchar` |  |  | Pay-by Address. Max length: 9 |
| `pfcn` | `string` | `varchar` |  |  | Pay-by Contact. Max length: 9 |
| `dfpb` | `integer` | `int` |  |  | Discounts from Price Books. Allowed values: 1, 2 |
| `dfpb_kw` | `string` | `varchar` |  |  | Discounts from Price Books (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bppr` | `string` | `varchar` |  |  | Business Partner Prices and Discounts. Max length: 9 |
| `creg` | `string` | `varchar` |  |  | Sales Area. Max length: 3 |
| `csmt` | `float` | `float` |  |  | Calculated Sales Amount |
| `rsmt` | `float` | `float` |  |  | Recommended Sales Amount |
| `camt_1` | `float` | `float` |  |  | Cost Amount |
| `camt_2` | `float` | `float` |  |  | Cost Amount |
| `camt_3` | `float` | `float` |  |  | Cost Amount |
| `rvmd` | `integer` | `int` |  |  | Revenue Recognition Based on. Allowed values: 5, 10, 15, 20, 50 |
| `rvmd_kw` | `string` | `varchar` |  |  | Revenue Recognition Based on (keyword). Allowed values: tsctm.rvmd.day.per.period, tsctm.rvmd.cumu.days, tsctm.rvmd.erf.per.period, tsctm.rvmd.erf.cumu.cost, tsctm.rvmd.not.appl |
| `rrcr` | `integer` | `int` |  |  | Recognize Revenue per Contract Renewal. Allowed values: 1, 2 |
| `rrcr_kw` | `string` | `varchar` |  |  | Recognize Revenue per Contract Renewal (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rccg` | `integer` | `int` |  |  | Recognize Revenue per Configuration. Allowed values: 1, 2 |
| `rccg_kw` | `string` | `varchar` |  |  | Recognize Revenue per Configuration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prov` | `float` | `float` |  |  | Provision |
| `bptc` | `string` | `varchar` |  |  | Business Partner Tax Country. Max length: 3 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ceno` | `string` | `varchar` |  |  | Tax Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Tax Exemption Reason. Max length: 6 |
| `ctik` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `inpc` | `float` | `float` |  |  | Obsolete |
| `txyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `txyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Quote Text |
| `txtb` | `integer` | `int` |  |  | Header Text |
| `txtc` | `integer` | `int` |  |  | Footer Text |
| `txtd` | `integer` | `int` |  |  | Installment Text |
| `ctpc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm005 Contract Types |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cplt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `psrc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `cind_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm060 Indexation Templates |
| `ctin_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm410 Installment Templates |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `itad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `itcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `pfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `pfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `bppr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `rsmt_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `rsmt_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `rsmt_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `rsmt_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `rsmt_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
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

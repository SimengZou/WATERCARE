# tppin010

LN tppin010 Advance Payments table, Generated 2026-04-10T19:42:03Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppin010` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln`, `sern`, `cprj`, `ofbp` |
| **Column count** | 94 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Advance Payment |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ofbp` | `string` | `varchar` | `PK` | `not_null` | (required) Sold-to Business Partner. Max length: 9 |
| `cnpr` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `tact` | `string` | `varchar` |  |  | Triggering Activity. Max length: 16 |
| `cpro` | `string` | `varchar` |  |  | Revenue Code. Max length: 8 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `amnt` | `float` | `float` |  |  | Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Sales Rate |
| `rate_2` | `float` | `float` |  |  | Sales Rate |
| `rate_3` | `float` | `float` |  |  | Sales Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rdas` | `timestamp` | `timestamp_ntz` |  |  | Rate Date (Sales) |
| `pdbo` | `integer` | `int` |  |  | Planned Invoice Date based on. Allowed values: 10, 20 |
| `pdbo_kw` | `string` | `varchar` |  |  | Planned Invoice Date based on (keyword). Allowed values: tpgen.strt.fin.start, tpgen.strt.fin.finish |
| `sidt` | `timestamp` | `timestamp_ntz` |  |  | Planned Invoice Date |
| `invo` | `integer` | `int` |  |  | Approved for Invoicing. Allowed values: 1, 2 |
| `invo_kw` | `string` | `varchar` |  |  | Approved for Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `nins` | `integer` | `int` |  |  | Installment |
| `oadv` | `integer` | `int` |  |  | Original Advance |
| `trsl` | `integer` | `int` |  |  | Transferred to Invoicing. Allowed values: 1, 2 |
| `trsl_kw` | `string` | `varchar` |  |  | Transferred to Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `dnse` | `integer` | `int` |  |  | Do Not Settle. Allowed values: 1, 2 |
| `dnse_kw` | `string` | `varchar` |  |  | Do Not Settle (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adsu` | `integer` | `int` |  |  | Advance Settled. Allowed values: 1, 2 |
| `adsu_kw` | `string` | `varchar` |  |  | Advance Settled (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `toin` | `integer` | `int` |  |  | Use Text on Invoice. Allowed values: 1, 2 |
| `toin_kw` | `string` | `varchar` |  |  | Use Text on Invoice (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `cvyn` | `integer` | `int` |  |  | Tax [y/n]. Allowed values: 0, 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Tax [y/n] (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `exmt` | `integer` | `int` |  |  | Tax Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Tax Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Text |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_cnln_nins_cprj_ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin020 Installments |
| `cono_cnln_oadv_cprj_ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin010 Advance Payments |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `cnpr_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cnpr_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cnpr_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cnpr_cpla_tact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cnpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `amnt_refc` | `float` | `float` |  |  | CC: Advance Amount in Reference Currency |
| `amnt_cntc` | `float` | `float` |  |  | CC: Advance Amount in Contract Currency |
| `amnt_prjc` | `float` | `float` |  |  | CC: Advance Amount in Project Currency |
| `amnt_homc` | `float` | `float` |  |  | CC: Advance Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Advance Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Advance Amount in Reporting Currency 2 |
| `amnt_dwhc` | `float` | `float` |  |  | CC: Advance Amount in Data Warehouse Currency |
| `cprj_cpro` | `string` | `varchar` |  |  | CC: Project Revenue Code. Max length: 25 |
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

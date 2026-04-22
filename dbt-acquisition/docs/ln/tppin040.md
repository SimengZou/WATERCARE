# tppin040

LN tppin040 Holdback table, Generated 2026-04-10T19:42:05Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppin040` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln`, `hbnr`, `cprj`, `ofbp` |
| **Column count** | 123 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
| `hbnr` | `integer` | `int` | `PK` | `not_null` | (required) Holdback Sequence Number |
| `osrn` | `integer` | `int` |  |  | Original Sequence Number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ofbp` | `string` | `varchar` | `PK` | `not_null` | (required) Sold-to Business Partner. Max length: 9 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `tact` | `string` | `varchar` |  |  | Triggering Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `task` | `string` | `varchar` |  |  | Labor. Max length: 8 |
| `cequ` | `string` | `varchar` |  |  | Equipment. Max length: 47 |
| `csub` | `string` | `varchar` |  |  | Subcontracting. Max length: 47 |
| `cico` | `string` | `varchar` |  |  | Sundry Cost. Max length: 8 |
| `ovhd` | `string` | `varchar` |  |  | Overhead. Max length: 8 |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `nins` | `integer` | `int` |  |  | Installment |
| `dlvr` | `integer` | `int` |  |  | Deliverable |
| `schd` | `integer` | `int` |  |  | Schedule |
| `gseq` | `integer` | `int` |  |  | General Sequence Number |
| `urdt` | `timestamp` | `timestamp_ntz` |  |  | Unit Rate Date |
| `cpro` | `string` | `varchar` |  |  | Revenue Code. Max length: 8 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `invo` | `integer` | `int` |  |  | Approved for Invoicing. Allowed values: 1, 2 |
| `invo_kw` | `string` | `varchar` |  |  | Approved for Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `hbpc` | `float` | `float` |  |  | Holdback Percentage |
| `hbai` | `float` | `float` |  |  | Holdback Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `pcwa` | `float` | `float` |  |  | Labor Part of Contract Amount |
| `sidt` | `timestamp` | `timestamp_ntz` |  |  | Cut Off Date |
| `trsl` | `integer` | `int` |  |  | Transferred to Invoicing. Allowed values: 1, 2 |
| `trsl_kw` | `string` | `varchar` |  |  | Transferred to Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `htai` | `float` | `float` |  |  | Amount to be Invoiced |
| `hiai` | `float` | `float` |  |  | Invoiced Amount |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `clos` | `integer` | `int` |  |  | Closed. Allowed values: 1, 2 |
| `clos_kw` | `string` | `varchar` |  |  | Closed (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cncl` | `integer` | `int` |  |  | Canceled. Allowed values: 1, 2 |
| `cncl_kw` | `string` | `varchar` |  |  | Canceled (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `toin` | `integer` | `int` |  |  | Use Text on Invoice. Allowed values: 1, 2 |
| `toin_kw` | `string` | `varchar` |  |  | Use Text on Invoice (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `disp` | `float` | `float` |  |  | Discount Percentage |
| `diam` | `float` | `float` |  |  | Discount Amount |
| `hbdc` | `string` | `varchar` |  |  | Holdback Discount Code. Max length: 3 |
| `orap` | `string` | `varchar` |  |  | Originating AFP. Max length: 9 |
| `reap` | `string` | `varchar` |  |  | Release AFP. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Text |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cpla_tact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `ovhd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm042 Standard Overhead |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `hbdc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs021 Discount Codes |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cprj_task` | `string` | `varchar` |  |  | CC: Project Labor. Max length: 25 |
| `cprj_cico` | `string` | `varchar` |  |  | CC: Project Sundry. Max length: 25 |
| `hbai_refc` | `float` | `float` |  |  | CC: Holdback Amount in Reference Currency |
| `hbai_cntc` | `float` | `float` |  |  | CC: Holdback Amount in Contract Currency |
| `hbai_prjc` | `float` | `float` |  |  | CC: Holdback Amount in Project Currency |
| `hbai_homc` | `float` | `float` |  |  | CC: Holdback Amount in Local Currency |
| `hbai_rpc1` | `float` | `float` |  |  | CC: Holdback Amount in Reporting Currency 1 |
| `hbai_rpc2` | `float` | `float` |  |  | CC: Holdback Amount in Reporting Currency 2 |
| `hbai_dwhc` | `float` | `float` |  |  | CC: Holdback Amount in Data Warehouse Currency |
| `htai_refc` | `float` | `float` |  |  | CC: Holdback to be Invoiced Amount in Reference Currency |
| `htai_cntc` | `float` | `float` |  |  | CC: Holdback to be Invoiced Amount in Contract Currency |
| `htai_prjc` | `float` | `float` |  |  | CC: Holdback to be Invoiced Amount in Project Currency |
| `htai_homc` | `float` | `float` |  |  | CC: Holdback to be Invoiced Amount in Local Currency |
| `htai_rpc1` | `float` | `float` |  |  | CC: Holdback to be Invoiced Amount in Reporting Currency 1 |
| `htai_rpc2` | `float` | `float` |  |  | CC: Holdback to be Invoiced Amount in Reporting Currency 2 |
| `htai_dwhc` | `float` | `float` |  |  | CC: Holdback to be Invoiced Amount in Data Warehouse Currency |
| `hiai_refc` | `float` | `float` |  |  | CC: Holdback Invoiced Amount in Reference Currency |
| `hiai_cntc` | `float` | `float` |  |  | CC: Holdback Invoiced Amount in Contract Currency |
| `hiai_prjc` | `float` | `float` |  |  | CC: Holdback Invoiced Amount in Project Currency |
| `hiai_homc` | `float` | `float` |  |  | CC: Holdback Invoiced Amount in Local Currency |
| `hiai_rpc1` | `float` | `float` |  |  | CC: Holdback Invoiced Amount in Reporting Currency 1 |
| `hiai_rpc2` | `float` | `float` |  |  | CC: Holdback Invoiced Amount in Reporting Currency 2 |
| `hiai_dwhc` | `float` | `float` |  |  | CC: Holdback Invoiced Amount in Data Warehouse Currency |
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

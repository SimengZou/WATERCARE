# tppin020

LN tppin020 Installments table, Generated 2026-04-10T19:42:04Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppin020` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `cnln`, `nins`, `cprj`, `ofbp` |
| **Column count** | 154 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `cnln` | `string` | `varchar` | `PK` | `not_null` | (required) Contract Line. Max length: 8 |
| `nins` | `integer` | `int` | `PK` | `not_null` | (required) Installment |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ofbp` | `string` | `varchar` | `PK` | `not_null` | (required) Sold-to Business Partner. Max length: 9 |
| `cnpr` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity, posting. Max length: 16 |
| `tact` | `string` | `varchar` |  |  | Triggering Activity. Max length: 16 |
| `cpro` | `string` | `varchar` |  |  | Revenue Code. Max length: 8 |
| `cpra` | `string` | `varchar` |  |  | Add. Work Revenue Code. Max length: 8 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Additional Work Description Installment - base language. Max length: 30 |
| `npoi` | `integer` | `int` |  |  | Number of Points |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `nper` | `float` | `float` |  |  | Percentage |
| `insa` | `float` | `float` |  |  | Installment Amount |
| `inaa` | `float` | `float` |  |  | Installm. Amnt Add.Wrk |
| `sidt` | `timestamp` | `timestamp_ntz` |  |  | Planned Invoice Date |
| `invo` | `integer` | `int` |  |  | Approved for Invoicing. Allowed values: 1, 2 |
| `invo_kw` | `string` | `varchar` |  |  | Approved for Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `inpo` | `integer` | `int` |  |  | Points to be Invoiced |
| `inpr` | `float` | `float` |  |  | Percentage to be Invoiced |
| `inia` | `float` | `float` |  |  | Installment Amount to be Invoiced |
| `iiaa` | `float` | `float` |  |  | Installment Amount Add. Work to be Invoiced |
| `disc` | `float` | `float` |  |  | Invoice Discount |
| `tipo` | `integer` | `int` |  |  | Points Invoiced |
| `tipr` | `float` | `float` |  |  | Invoiced Percentage |
| `tiia` | `float` | `float` |  |  | Invoiced Amount |
| `tiaa` | `float` | `float` |  |  | Invoiced Amount Additional Work |
| `cvyn` | `integer` | `int` |  |  | Tax [y/n]. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Tax [y/n] (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `pfdt` | `timestamp` | `timestamp_ntz` |  |  | Date of Performance |
| `peru` | `timestamp` | `timestamp_ntz` |  |  | Progress Invoice Period End |
| `fins` | `integer` | `int` |  |  | Final Installment. Allowed values: 1, 2 |
| `fins_kw` | `string` | `varchar` |  |  | Final Installment (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `trsl` | `integer` | `int` |  |  | Transferred to Invoicing. Allowed values: 1, 2 |
| `trsl_kw` | `string` | `varchar` |  |  | Transferred to Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `fcma` | `integer` | `int` |  |  | Fin.Comp Addit.Work |
| `itpa` | `string` | `varchar` |  |  | Transaction Type Additional Work. Max length: 3 |
| `idca` | `integer` | `int` |  |  | Invoice Document Additional Work |
| `hbyn` | `integer` | `int` |  |  | Use Holdback. Allowed values: 1, 2 |
| `hbyn_kw` | `string` | `varchar` |  |  | Use Holdback (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `hbpc` | `float` | `float` |  |  | Holdback Percentage |
| `hbai` | `float` | `float` |  |  | Holdback Amount |
| `hbnr` | `integer` | `int` |  |  | Holdback Sequence Number |
| `clos` | `integer` | `int` |  |  | Closed. Allowed values: 1, 2 |
| `clos_kw` | `string` | `varchar` |  |  | Closed (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `toin` | `integer` | `int` |  |  | Use Text on Invoice. Allowed values: 1, 2 |
| `toin_kw` | `string` | `varchar` |  |  | Use Text on Invoice (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `tcst` | `integer` | `int` |  |  | Document Compliance Status. Allowed values: 10, 20, 30, 40, 50 |
| `tcst_kw` | `string` | `varchar` |  |  | Document Compliance Status (keyword). Allowed values: tcgtc.tcst.to.be.validated, tcgtc.tcst.validating, tcgtc.tcst.valid.error, tcgtc.tcst.validated, tcgtc.tcst.not.appl |
| `cstv` | `float` | `float` |  |  | Customs Value for Normal Work |
| `catv` | `float` | `float` |  |  | Customs Value for Additional Work |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `srdc` | `string` | `varchar` |  |  | Source Document. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Text |
| `cono_cnln_hbnr_cprj_ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin040 Holdback |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `cnpr_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cnpr_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cnpr_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cnpr_cpla_tact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cnpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `insa_refc` | `float` | `float` |  |  | CC: Installment Amount in Reference Currency |
| `insa_cntc` | `float` | `float` |  |  | CC: Installment Amount in Contract Currency |
| `insa_prjc` | `float` | `float` |  |  | CC: Installment Amount in Project Currency |
| `insa_homc` | `float` | `float` |  |  | CC: Installment Amount in Local Currency |
| `insa_rpc1` | `float` | `float` |  |  | CC: Installment Amount in Reporting Currency 1 |
| `insa_rpc2` | `float` | `float` |  |  | CC: Installment Amount in Reporting Currency 2 |
| `insa_dwhc` | `float` | `float` |  |  | CC: Installment Amount in Data Warehouse Currency |
| `inia_refc` | `float` | `float` |  |  | CC: Installment Approved Amount in Reference Currency |
| `inia_cntc` | `float` | `float` |  |  | CC: Installment Approved Amount in Contract Currency |
| `inia_prjc` | `float` | `float` |  |  | CC: Installment Approved Amount in Project Currency |
| `inia_homc` | `float` | `float` |  |  | CC: Installment Approved Amount in Local Currency |
| `inia_rpc1` | `float` | `float` |  |  | CC: Installment Approved Amount in Reporting Currency 1 |
| `inia_rpc2` | `float` | `float` |  |  | CC: Installment Approved Amount in Reporting Currency 2 |
| `inia_dwhc` | `float` | `float` |  |  | CC: Installment Approved Amount in Data Warehouse Currency |
| `tiia_refc` | `float` | `float` |  |  | CC: Installment Invoiced Amount in Reference Currency |
| `tiia_cntc` | `float` | `float` |  |  | CC: Installment Invoiced Amount in Contract Currency |
| `tiia_prjc` | `float` | `float` |  |  | CC: Installment Invoiced Amount in Project Currency |
| `tiia_homc` | `float` | `float` |  |  | CC: Installment Invoiced Amount in Local Currency |
| `tiia_rpc1` | `float` | `float` |  |  | CC: Installment Invoiced Amount in Reporting Currency 1 |
| `tiia_rpc2` | `float` | `float` |  |  | CC: Installment Invoiced Amount in Reporting Currency 2 |
| `tiia_dwhc` | `float` | `float` |  |  | CC: Installment Invoiced Amount in Data Warehouse Currency |
| `inaa_refc` | `float` | `float` |  |  | CC: Additional Work Installment Amount in Reference Currency |
| `inaa_cntc` | `float` | `float` |  |  | CC: Additional Work Installment Amount in Contract Currency |
| `inaa_prjc` | `float` | `float` |  |  | CC: Additional Work Installment Amount in Project Currency |
| `inaa_homc` | `float` | `float` |  |  | CC: Additional Work Installment Amount in Local Currency |
| `inaa_rpc1` | `float` | `float` |  |  | CC: Additional Work Installment Amount in Reporting Currency 1 |
| `inaa_rpc2` | `float` | `float` |  |  | CC: Additional Work Installment Amount in Reporting Currency 2 |
| `inaa_dwhc` | `float` | `float` |  |  | CC: Additional Work Installment Amount in Data Warehouse Currency |
| `iiaa_refc` | `float` | `float` |  |  | CC: Additional Work Approved Amount in Reference Currency |
| `iiaa_cntc` | `float` | `float` |  |  | CC: Additional Work Approved Amount in Contract Currency |
| `iiaa_prjc` | `float` | `float` |  |  | CC: Additional Work Approved Amount in Project Currency |
| `iiaa_homc` | `float` | `float` |  |  | CC: Additional Work Approved Amount in Local Currency |
| `iiaa_rpc1` | `float` | `float` |  |  | CC: Additional Work Approved Amount in Reporting Currency 1 |
| `iiaa_rpc2` | `float` | `float` |  |  | CC: Additional Work Approved Amount in Reporting Currency 2 |
| `iiaa_dwhc` | `float` | `float` |  |  | CC: Additional Work Approved Amount in Data Warehouse Currency |
| `tiaa_refc` | `float` | `float` |  |  | CC: Additional Work Invoiced Amount in Reference Currency |
| `tiaa_cntc` | `float` | `float` |  |  | CC: Additional Work Invoiced Amount in Contract Currency |
| `tiaa_prjc` | `float` | `float` |  |  | CC: Additional Work Invoiced Amount in Project Currency |
| `tiaa_homc` | `float` | `float` |  |  | CC: Additional Work Invoiced Amount in Local Currency |
| `tiaa_rpc1` | `float` | `float` |  |  | CC: Additional Work Invoiced Amount in Reporting Currency 1 |
| `tiaa_rpc2` | `float` | `float` |  |  | CC: Additional Work Invoiced Amount in Reporting Currency 2 |
| `tiaa_dwhc` | `float` | `float` |  |  | CC: Additional Work Invoiced Amount in Data Warehouse Currency |
| `cprj_cpro` | `string` | `varchar` |  |  | CC: Project Revenue Code. Max length: 25 |
| `cprj_cpra` | `string` | `varchar` |  |  | CC: Additional Work Revenue Code. Max length: 25 |
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

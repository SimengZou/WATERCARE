# tpppc350

LN tpppc350 Interim Results table, Generated 2026-04-10T19:42:12Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc350` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cstl`, `cact`, `cspa`, `sern` |
| **Column count** | 193 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cstl` | `string` | `varchar` | `PK` | `not_null` | (required) Extension. Max length: 4 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cnln` | `string` | `varchar` |  |  | Contract Line. Max length: 8 |
| `cprv` | `string` | `varchar` |  |  | Financial Result Type (obsolete). Max length: 3 |
| `rtre` | `string` | `varchar` |  |  | Result Type Revenue. Max length: 3 |
| `rtco` | `string` | `varchar` |  |  | Result Type Cost. Max length: 3 |
| `prvt` | `integer` | `int` |  |  | Result Type (obsolete). Allowed values: 1, 2, 3 |
| `prvt_kw` | `string` | `varchar` |  |  | Result Type (obsolete) (keyword). Allowed values: tppdm.prvt.costs, tppdm.prvt.results, tppdm.prvt.balance |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type (obsolete). Max length: 3 |
| `rert` | `string` | `varchar` |  |  | Revenue - Exchange Rate Type. Max length: 3 |
| `cert` | `string` | `varchar` |  |  | Cost - Exchange Rate Type. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Currency Rate (obsolete) |
| `rate_2` | `float` | `float` |  |  | Currency Rate (obsolete) |
| `rate_3` | `float` | `float` |  |  | Currency Rate (obsolete) |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor  (obsolete) |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor  (obsolete) |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor  (obsolete) |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `amnt` | `float` | `float` |  |  | Amount |
| `amhc_1` | `float` | `float` |  |  | Amount Home Currency (obsolete) |
| `amhc_2` | `float` | `float` |  |  | Amount Home Currency (obsolete) |
| `amhc_3` | `float` | `float` |  |  | Amount Home Currency (obsolete) |
| `rrby` | `integer` | `int` |  |  | Revenue Recognition by. Allowed values: 10, 20, 30 |
| `rrby_kw` | `string` | `varchar` |  |  | Revenue Recognition by (keyword). Allowed values: tpctm.rrby.contract, tpctm.rrby.contract.line, tpctm.rrby.project |
| `irby` | `integer` | `int` |  |  | Interim Result by. Allowed values: 5, 10 |
| `irby_kw` | `string` | `varchar` |  |  | Interim Result by (keyword). Allowed values: tpctm.irby.project, tpctm.irby.contract.line |
| `irsc` | `integer` | `int` |  |  | Scenario. Allowed values: 5, 10, 100 |
| `irsc_kw` | `string` | `varchar` |  |  | Scenario (keyword). Allowed values: tpctm.irsc.primary, tpctm.irsc.alternate, tpctm.irsc.not.applicable |
| `scds__en_us` | `string` | `varchar` |  | `not_null` | (required) Scenario Description - base language. Max length: 30 |
| `pfob` | `integer` | `int` |  |  | Base for Performance Obligation. Allowed values: 5, 10, 15 |
| `pfob_kw` | `string` | `varchar` |  |  | Base for Performance Obligation (keyword). Allowed values: tpctm.pfob.contract.amount, tpctm.pfob.trans.price, tpctm.pfob.not.applicable |
| `revr` | `integer` | `int` |  |  | Revenue Recognition Method. Allowed values: 1, 2, 3, 4, 5, 6, 20 |
| `revr_kw` | `string` | `varchar` |  |  | Revenue Recognition Method (keyword). Allowed values: tppdm.revr.cmpl, tppdm.revr.perc, tppdm.revr.mils, tppdm.revr.rebu, tppdm.revr.erfc, tppdm.revr.actr, tppdm.revr.na |
| `cmpc` | `integer` | `int` |  |  | Calculation Method for Percentage of Completion. Allowed values: 10, 20, 100, 200 |
| `cmpc_kw` | `string` | `varchar` |  |  | Calculation Method for Percentage of Completion (keyword). Allowed values: tppdm.cmpc.cost.to.cost, tppdm.cmpc.hours.progress, tppdm.cmpc.manually, tppdm.cmpc.na |
| `pcmp` | `float` | `float` |  |  | Percentage of Completion |
| `bfrf` | `integer` | `int` |  |  | Base for Earned Revenue Factor. Allowed values: 10, 20, 30, 100 |
| `bfrf_kw` | `string` | `varchar` |  |  | Base for Earned Revenue Factor (keyword). Allowed values: tppdm.bfpp.estimate.compl, tppdm.bfpp.budget, tppdm.bfpp.manually, tppdm.bfpp.not.app |
| `earf` | `float` | `float` |  |  | Earned Revenue Factor |
| `revt` | `float` | `float` |  |  | Revenue Recognition Threshold |
| `revl` | `float` | `float` |  |  | Revenue Recognition Limit |
| `cogs` | `integer` | `int` |  |  | Costs of Sales Method. Allowed values: 1, 2, 3, 10, 20 |
| `cogs_kw` | `string` | `varchar` |  |  | Costs of Sales Method (keyword). Allowed values: tppdm.cogs.cmpl, tppdm.cogs.prop, tppdm.cogs.rebu, tppdm.cogs.perc, tppdm.cogs.na |
| `bfpp` | `integer` | `int` |  |  | Base for Profit Percentage. Allowed values: 10, 20, 30, 100 |
| `bfpp_kw` | `string` | `varchar` |  |  | Base for Profit Percentage (keyword). Allowed values: tppdm.bfpp.estimate.compl, tppdm.bfpp.budget, tppdm.bfpp.manually, tppdm.bfpp.not.app |
| `prpc` | `float` | `float` |  |  | Profit Percentage |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `cptc` | `string` | `varchar` |  |  | Period Table Cost Control. Max length: 6 |
| `year` | `integer` | `int` |  |  | Cost Control Year |
| `peri` | `integer` | `int` |  |  | Cost Control Period |
| `cptf` | `string` | `varchar` |  |  | Financial Period Table. Max length: 6 |
| `fyea` | `integer` | `int` |  |  | Financial Year |
| `fper` | `integer` | `int` |  |  | Financial Period |
| `cfpo` | `integer` | `int` |  |  | Approved for Posting. Allowed values: 1, 2 |
| `cfpo_kw` | `string` | `varchar` |  |  | Approved for Posting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `appr` | `string` | `varchar` |  |  | Approver. Max length: 16 |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `apdt` | `timestamp` | `timestamp_ntz` |  |  | Approval Date |
| `post` | `integer` | `int` |  |  | Revenue and Cost Posted. Allowed values: 1, 2 |
| `post_kw` | `string` | `varchar` |  |  | Revenue and Cost Posted (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `pstb` | `integer` | `int` |  |  | Balance Posted. Allowed values: 1, 2 |
| `pstb_kw` | `string` | `varchar` |  |  | Balance Posted (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `afin` | `integer` | `int` |  |  | Alternate Results Finalized. Allowed values: 1, 2 |
| `afin_kw` | `string` | `varchar` |  |  | Alternate Results Finalized (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `comf` | `integer` | `int` |  |  | Revenue and Cost Reported Completed. Allowed values: 1, 2, 3 |
| `comf_kw` | `string` | `varchar` |  |  | Revenue and Cost Reported Completed (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `cmfb` | `integer` | `int` |  |  | Balance Reported Completed. Allowed values: 1, 2, 3 |
| `cmfb_kw` | `string` | `varchar` |  |  | Balance Reported Completed (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `drun` | `timestamp` | `timestamp_ntz` |  |  | Run Date |
| `serb` | `integer` | `int` |  |  | Run |
| `cdru` | `timestamp` | `timestamp_ntz` |  |  | Run Date (Report Completed) |
| `cser` | `integer` | `int` |  |  | Run Sequence No. (Report Completed) |
| `frgd` | `timestamp` | `timestamp_ntz` |  |  | Financial Registration Date |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `guid` | `string` | `varchar` |  |  | Unique ID. Max length: 22 |
| `iror` | `integer` | `int` |  |  | Origin. Allowed values: 5, 10, 15 |
| `iror_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tpppc.iror.generated, tpppc.iror.manual, tpppc.iror.reversal |
| `copr` | `float` | `float` |  |  | Contract Amount |
| `trpr` | `float` | `float` |  |  | Transaction Price |
| `btcs` | `float` | `float` |  |  | Billed to Customer |
| `exrv` | `float` | `float` |  |  | Expected Revenue |
| `farv` | `float` | `float` |  |  | Forecast Additional Revenue |
| `bgam` | `float` | `float` |  |  | Budgeted Amount |
| `ctic` | `float` | `float` |  |  | Cost Incurred |
| `etcm` | `float` | `float` |  |  | Estimate to Complete |
| `estc` | `float` | `float` |  |  | Estimate at Completion |
| `clmg` | `float` | `float` |  |  | Calculated Margin |
| `blnc` | `float` | `float` |  |  | Balance |
| `hrbg` | `float` | `float` |  |  | Budget Hours |
| `hrct` | `float` | `float` |  |  | Hours Cost |
| `hetc` | `float` | `float` |  |  | Hours Estimate to Complete |
| `heac` | `float` | `float` |  |  | Hours Estimate at Completion |
| `ramc` | `float` | `float` |  |  | Revenue Calculated |
| `rhcc_1` | `float` | `float` |  |  | Revenue Amount Calculated in Home Currency |
| `rhcc_2` | `float` | `float` |  |  | Revenue Amount Calculated in Home Currency |
| `rhcc_3` | `float` | `float` |  |  | Revenue Amount Calculated in Home Currency |
| `rrdc` | `timestamp` | `timestamp_ntz` |  |  | Revenue - Exchange Rate Date (at Time of Calculation) |
| `rrtc_1` | `float` | `float` |  |  | Revenue - Exchange Rate Calculated |
| `rrtc_2` | `float` | `float` |  |  | Revenue - Exchange Rate Calculated |
| `rrtc_3` | `float` | `float` |  |  | Revenue - Exchange Rate Calculated |
| `rrfc_1` | `integer` | `int` |  |  | Revenue - Exchange Rate Factor Calculated |
| `rrfc_2` | `integer` | `int` |  |  | Revenue - Exchange Rate Factor Calculated |
| `rrfc_3` | `integer` | `int` |  |  | Revenue - Exchange Rate Factor Calculated |
| `ramr` | `float` | `float` |  |  | Revenue Recognized |
| `rhcr_1` | `float` | `float` |  |  | Revenue Amount Recognized in Home Currency |
| `rhcr_2` | `float` | `float` |  |  | Revenue Amount Recognized in Home Currency |
| `rhcr_3` | `float` | `float` |  |  | Revenue Amount Recognized in Home Currency |
| `rrdr` | `timestamp` | `timestamp_ntz` |  |  | Revenue - Exchange Rate Date Recognized |
| `rrtr_1` | `float` | `float` |  |  | Revenue - Exchange Rate Recognized |
| `rrtr_2` | `float` | `float` |  |  | Revenue - Exchange Rate Recognized |
| `rrtr_3` | `float` | `float` |  |  | Revenue - Exchange Rate Recognized |
| `rrfr_1` | `integer` | `int` |  |  | Revenue - Exchange Rate Factor Recognized |
| `rrfr_2` | `integer` | `int` |  |  | Revenue - Exchange Rate Factor Recognized |
| `rrfr_3` | `integer` | `int` |  |  | Revenue - Exchange Rate Factor Recognized |
| `camc` | `float` | `float` |  |  | Cost Calculated |
| `chcc_1` | `float` | `float` |  |  | Cost Amount Calculated in Home Currency |
| `chcc_2` | `float` | `float` |  |  | Cost Amount Calculated in Home Currency |
| `chcc_3` | `float` | `float` |  |  | Cost Amount Calculated in Home Currency |
| `crdc` | `timestamp` | `timestamp_ntz` |  |  | Cost - Exchange Rate Date (at Time of Calculation) |
| `crtc_1` | `float` | `float` |  |  | Cost - Exchange Rate Calculated |
| `crtc_2` | `float` | `float` |  |  | Cost - Exchange Rate Calculated |
| `crtc_3` | `float` | `float` |  |  | Cost - Exchange Rate Calculated |
| `crfc_1` | `integer` | `int` |  |  | Cost - Exchange Rate Factor Calculated |
| `crfc_2` | `integer` | `int` |  |  | Cost - Exchange Rate Factor Calculated |
| `crfc_3` | `integer` | `int` |  |  | Cost - Exchange Rate Factor Calculated |
| `camr` | `float` | `float` |  |  | Cost Amount Recognized |
| `chcr_1` | `float` | `float` |  |  | Cost Amount Recognized in Home Currency |
| `chcr_2` | `float` | `float` |  |  | Cost Amount Recognized in Home Currency |
| `chcr_3` | `float` | `float` |  |  | Cost Amount Recognized in Home Currency |
| `crdr` | `timestamp` | `timestamp_ntz` |  |  | Cost - Exchange Rate Date Recognized |
| `crtr_1` | `float` | `float` |  |  | Cost - Exchange Rate Recognized |
| `crtr_2` | `float` | `float` |  |  | Cost - Exchange Rate Recognized |
| `crtr_3` | `float` | `float` |  |  | Cost - Exchange Rate Recognized |
| `crfr_1` | `integer` | `int` |  |  | Cost - Exchange Rate Factor Recognized |
| `crfr_2` | `integer` | `int` |  |  | Cost - Exchange Rate Factor Recognized |
| `crfr_3` | `integer` | `int` |  |  | Cost - Exchange Rate Factor Recognized |
| `txtn` | `integer` | `int` |  |  | Text |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `cprv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm155 Financial Result Types |
| `rtre_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm155 Financial Result Types |
| `rtco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm155 Financial Result Types |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `rert_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cert_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptf_fyea_fper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `txtn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `blnc_refc` | `float` | `float` |  |  | CC: Balance in Reference Currency |
| `blnc_prjc` | `float` | `float` |  |  | CC: Balance in Project Currency |
| `blnc_cntc` | `float` | `float` |  |  | CC: Balance in Contract Currency |
| `blnc_dwhc` | `float` | `float` |  |  | CC: Balance in Data Warehouse Currency |
| `blnc_homc` | `float` | `float` |  |  | CC: Balance in Local Currency |
| `blnc_rpc1` | `float` | `float` |  |  | CC: Balance in Reporting Currency 1 |
| `blnc_rpc2` | `float` | `float` |  |  | CC: Balance in Reporting Currency 2 |
| `ramc_refc` | `float` | `float` |  |  | CC: Revenue Amount in Reference Currency |
| `ramc_prjc` | `float` | `float` |  |  | CC: Revenue Amount in Project Currency |
| `ramc_cntc` | `float` | `float` |  |  | CC: Revenue Amount in Contract Currency |
| `ramc_dwhc` | `float` | `float` |  |  | CC: Revenue Amount in Data Warehouse Currency |
| `ramr_refc` | `float` | `float` |  |  | CC: Revenue Amount Recognized in Reference Currency |
| `ramr_prjc` | `float` | `float` |  |  | CC: Revenue Amount Recognized in Project Currency |
| `ramr_cntc` | `float` | `float` |  |  | CC: Revenue Amount Recognized in Contract Currency |
| `ramr_dwhc` | `float` | `float` |  |  | CC: Revenue Amount Recognized in Data Warehouse Currency |
| `camc_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `camc_prjc` | `float` | `float` |  |  | CC: Cost Amount in Project Currency |
| `camc_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `camc_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
| `camr_refc` | `float` | `float` |  |  | CC: Cost Amount Recognized in Reference Currency |
| `camr_prjc` | `float` | `float` |  |  | CC: Cost Amount Recognized in Project Currency |
| `camr_cntc` | `float` | `float` |  |  | CC: Cost Amount Recognized in Contract Currency |
| `camr_dwhc` | `float` | `float` |  |  | CC: Cost Amount Recognized in Data Warehouse Currency |
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

# tpptc050

LN tpptc050 Extensions table, Generated 2026-04-10T19:42:20Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc050` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cstl` |
| **Column count** | 146 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cstl` | `string` | `varchar` | `PK` | `not_null` | (required) Extension. Max length: 4 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `stlt` | `integer` | `int` |  |  | Extension Type. Allowed values: 1, 2, 3, 4 |
| `stlt_kw` | `string` | `varchar` |  |  | Extension Type (keyword). Allowed values: tpptc.stlt.additional.work, tpptc.stlt.provision.entry, tpptc.stlt.risc.settl, tpptc.stlt.quan.settl |
| `stlm` | `integer` | `int` |  |  | Invoice Calculation Method. Allowed values: 1, 2, 3 |
| `stlm_kw` | `string` | `varchar` |  |  | Invoice Calculation Method (keyword). Allowed values: tpptc.stlm.contract.price, tpptc.stlm.techn.budget, tpptc.stlm.real.costs |
| `stls` | `integer` | `int` |  |  | Extension Status. Allowed values: 1, 2, 3, 4, 5 |
| `stls_kw` | `string` | `varchar` |  |  | Extension Status (keyword). Allowed values: tpptc.stls.free, tpptc.stls.actual, tpptc.stls.definite, tpptc.stls.invoicing.allow, tpptc.stls.invoiced |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `excc` | `integer` | `int` |  |  | Cost Control. Allowed values: 1, 2 |
| `excc_kw` | `string` | `varchar` |  |  | Cost Control (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `lpsl` | `integer` | `int` |  |  | Phys. Progr. Registration. Allowed values: 0, 1, 2, 10 |
| `lpsl_kw` | `string` | `varchar` |  |  | Phys. Progr. Registration (keyword). Allowed values: , tppdm.lpsl.settlement, tppdm.lpsl.settl.costctr, tppdm.lpsl.not.applicable |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `idoc` | `integer` | `int` |  |  | Invoice Document |
| `ityp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `trsl` | `integer` | `int` |  |  | Transferred to Invoicing. Allowed values: 1, 2 |
| `trsl_kw` | `string` | `varchar` |  |  | Transferred to Invoicing (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `prva` | `float` | `float` |  |  | Provisional Amount |
| `pacu` | `string` | `varchar` |  |  | Provisional Currency. Max length: 3 |
| `copr` | `float` | `float` |  |  | Contract Amount |
| `cpcu` | `string` | `varchar` |  |  | Contract Currency. Max length: 3 |
| `codt` | `timestamp` | `timestamp_ntz` |  |  | Contract Date |
| `escu` | `string` | `varchar` |  |  | Expected Sales Currency. Max length: 3 |
| `esam` | `float` | `float` |  |  | Expected Sales Amount |
| `ceil` | `integer` | `int` |  |  | Ceiling. Allowed values: 1, 2 |
| `ceil_kw` | `string` | `varchar` |  |  | Ceiling (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `ivam` | `float` | `float` |  |  | Invoiced Amount |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Time |
| `fidt` | `timestamp` | `timestamp_ntz` |  |  | Finish Time |
| `chic` | `integer` | `int` |  |  | Project Surcharges. Allowed values: 1, 2 |
| `chic_kw` | `string` | `varchar` |  |  | Project Surcharges (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `csst` | `integer` | `int` |  |  | Provisional Amount Settled. Allowed values: 1, 2 |
| `csst_kw` | `string` | `varchar` |  |  | Provisional Amount Settled (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cadc` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `sadr` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `cres` | `string` | `varchar` |  |  | Responsibility. Max length: 3 |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `ccam` | `string` | `varchar` |  |  | Acquiring Method. Max length: 3 |
| `cfac` | `string` | `varchar` |  |  | Financing Method. Max length: 6 |
| `creg` | `string` | `varchar` |  |  | Geographical Area. Max length: 3 |
| `ccat` | `string` | `varchar` |  |  | Category. Max length: 6 |
| `csec` | `string` | `varchar` |  |  | Business Sector. Max length: 3 |
| `cstg` | `string` | `varchar` |  |  | Phase. Max length: 3 |
| `njsp` | `integer` | `int` |  |  | Number of Job Sheets Printed |
| `hbnr` | `integer` | `int` |  |  | Holdback Sequence Number |
| `hbyn` | `integer` | `int` |  |  | Use Holdback. Allowed values: 1, 2 |
| `hbyn_kw` | `string` | `varchar` |  |  | Use Holdback (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rtcp_1` | `float` | `float` |  |  | Currency Rate |
| `rtcp_2` | `float` | `float` |  |  | Currency Rate |
| `rtcp_3` | `float` | `float` |  |  | Currency Rate |
| `rtfp_1` | `integer` | `int` |  |  | Rate Factor |
| `rtfp_2` | `integer` | `int` |  |  | Rate Factor |
| `rtfp_3` | `integer` | `int` |  |  | Rate Factor |
| `rdap` | `timestamp` | `timestamp_ntz` |  |  | Rate Date Provisional amount |
| `rtcc_1` | `float` | `float` |  |  | Currency Rate |
| `rtcc_2` | `float` | `float` |  |  | Currency Rate |
| `rtcc_3` | `float` | `float` |  |  | Currency Rate |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `pscd__en_us` | `string` | `varchar` |  | `not_null` | (required) Primary Scenario Description - base language. Max length: 30 |
| `revr` | `integer` | `int` |  |  | Revenue Recognition Method. Allowed values: 1, 2, 3, 4, 5, 6, 20 |
| `revr_kw` | `string` | `varchar` |  |  | Revenue Recognition Method (keyword). Allowed values: tppdm.revr.cmpl, tppdm.revr.perc, tppdm.revr.mils, tppdm.revr.rebu, tppdm.revr.erfc, tppdm.revr.actr, tppdm.revr.na |
| `pcmp` | `float` | `float` |  |  | Percentage of Completion |
| `earf` | `float` | `float` |  |  | Earned Revenue Factor |
| `revt` | `float` | `float` |  |  | Revenue Recognition Threshold |
| `revl` | `float` | `float` |  |  | Revenue Recognition Limit |
| `cogs` | `integer` | `int` |  |  | Costs of Sales Method. Allowed values: 1, 2, 3, 10, 20 |
| `cogs_kw` | `string` | `varchar` |  |  | Costs of Sales Method (keyword). Allowed values: tppdm.cogs.cmpl, tppdm.cogs.prop, tppdm.cogs.rebu, tppdm.cogs.perc, tppdm.cogs.na |
| `prpc` | `float` | `float` |  |  | Profit Percentage |
| `ascd__en_us` | `string` | `varchar` |  | `not_null` | (required) Alternate Scenario Description - base language. Max length: 30 |
| `arrm` | `integer` | `int` |  |  | Alternate Revenue Recognition Method. Allowed values: 1, 2, 3, 4, 5, 6, 20 |
| `arrm_kw` | `string` | `varchar` |  |  | Alternate Revenue Recognition Method (keyword). Allowed values: tppdm.revr.cmpl, tppdm.revr.perc, tppdm.revr.mils, tppdm.revr.rebu, tppdm.revr.erfc, tppdm.revr.actr, tppdm.revr.na |
| `arpc` | `float` | `float` |  |  | Alternate Revenue Percentage of Completion |
| `aerf` | `float` | `float` |  |  | Alternate Earned Revenue Factor |
| `arrt` | `float` | `float` |  |  | Alternate Revenue Recognition Threshold |
| `arrl` | `float` | `float` |  |  | Alternate Revenue Recognition Limit |
| `acgm` | `integer` | `int` |  |  | Alternate Cost of Sales Method. Allowed values: 1, 2, 3, 10, 20 |
| `acgm_kw` | `string` | `varchar` |  |  | Alternate Cost of Sales Method (keyword). Allowed values: tppdm.cogs.cmpl, tppdm.cogs.prop, tppdm.cogs.rebu, tppdm.cogs.perc, tppdm.cogs.na |
| `aprp` | `float` | `float` |  |  | Alternate Profit Percentage |
| `styp` | `string` | `varchar` |  |  | Sales Type. Max length: 6 |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `pacu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cpcu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `escu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `cadc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cres_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm048 Responsibilities |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `ccam_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm063 Acquiring Methods |
| `cfac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm059 Financing Methods |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `ccat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm075 Categories |
| `csec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm055 Business Sectors |
| `cstg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm085 Phases |
| `styp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs202 Sales Types |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `prva_cntc` | `float` | `float` |  |  | CC: Provisional Amount in Contract Currency |
| `prva_refc` | `float` | `float` |  |  | CC: Provisional Amount in Reference Currency |
| `copr_refc` | `float` | `float` |  |  | CC: Extension Contract Amount in Reference Currency |
| `esam_refc` | `float` | `float` |  |  | CC: Expected Sales Amount in Reference Currency |
| `ivam_refc` | `float` | `float` |  |  | CC: Invoiced Amount in Reference Currency |
| `prva_prjc` | `float` | `float` |  |  | CC: Provisional Amount in Project Currency |
| `copr_prjc` | `float` | `float` |  |  | CC: Extension Contract Amount in Project Currency |
| `copr_cntc` | `float` | `float` |  |  | CC: Extension Contract Amount in Contract Currency |
| `esam_prjc` | `float` | `float` |  |  | CC: Expected Sales Amount in Project Currency |
| `esam_cntc` | `float` | `float` |  |  | CC: Expected Sales Amount in Contract Currency |
| `ivam_prjc` | `float` | `float` |  |  | CC: Invoiced Amount in Project Currency |
| `ivam_cntc` | `float` | `float` |  |  | CC: Invoiced Amount in Contract Currency |
| `prva_homc` | `float` | `float` |  |  | CC: Provisional Amount in Local Currency |
| `prva_rpc1` | `float` | `float` |  |  | CC: Provisional Amount in Reporting Currency 1 |
| `prva_rpc2` | `float` | `float` |  |  | CC: Provisional Amount in Reporting Currency 2 |
| `copr_homc` | `float` | `float` |  |  | CC: Extension Contract Amount in Local Currency |
| `copr_rpc1` | `float` | `float` |  |  | CC: Extension Contract Amount in Reporting Currency 1 |
| `copr_rpc2` | `float` | `float` |  |  | CC: Extension Contract Amount in Reporting Currency 2 |
| `esam_homc` | `float` | `float` |  |  | CC: Expected Sales Amount in Local Currency |
| `esam_rpc1` | `float` | `float` |  |  | CC: Expected Sales Amount in Reporting Currency 1 |
| `esam_rpc2` | `float` | `float` |  |  | CC: Expected Sales Amount in Reporting Currency 2 |
| `ivam_homc` | `float` | `float` |  |  | CC: Invoiced Amount in Local Currency |
| `ivam_rpc1` | `float` | `float` |  |  | CC: Invoiced Amount in Reporting Currency 1 |
| `ivam_rpc2` | `float` | `float` |  |  | CC: Invoiced Amount in Reporting Currency 2 |
| `prva_dwhc` | `float` | `float` |  |  | CC: Provisional Amount in Data Warehouse Currency |
| `copr_dwhc` | `float` | `float` |  |  | CC: Contract Amount in Data Warehouse Currency |
| `esam_dwhc` | `float` | `float` |  |  | CC: Estimate Sales Amount Amount in Data Warehouse Currency |
| `ivam_dwhc` | `float` | `float` |  |  | CC: Invoice Amount in Data Warehouse Currency |
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

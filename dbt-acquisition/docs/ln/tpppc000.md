# tpppc000

LN tpppc000 Parameters Project Production Control (PPC) table, Generated 2026-04-10T19:42:06Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc000` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt` |
| **Column count** | 102 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Introduction Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `pacc` | `integer` | `int` |  |  | Actual Cost Control Period. Allowed values: 10, 20, 30 |
| `pacc_kw` | `string` | `varchar` |  |  | Actual Cost Control Period (keyword). Allowed values: tpppc.periods.specific.period, tpppc.periods.current.period, tpppc.periods.previous.period |
| `year` | `integer` | `int` |  |  | Year of Actual Cost Control |
| `peri` | `integer` | `int` |  |  | Period for Actual Cost Control |
| `hyea` | `integer` | `int` |  |  | Year for Hours Control |
| `hper` | `integer` | `int` |  |  | Actual Hours Control Period |
| `pfpp` | `integer` | `int` |  |  | Obsolete (Cost Control Period = Fiscal Period). Allowed values: 1, 2 |
| `pfpp_kw` | `string` | `varchar` |  |  | Obsolete (Cost Control Period = Fiscal Period) (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `pgth` | `integer` | `int` |  |  | Physical Progress > 100% Allowed. Allowed values: 0, 1, 2 |
| `pgth_kw` | `string` | `varchar` |  |  | Physical Progress > 100% Allowed (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `copr` | `integer` | `int` |  |  | Contract Amount in WIP. Allowed values: 1, 2, 3 |
| `copr_kw` | `string` | `varchar` |  |  | Contract Amount in WIP (keyword). Allowed values: tpppc.dwip.plus, tpppc.dwip.minus, tpppc.dwip.not |
| `amoc` | `integer` | `int` |  |  | Costs in WIP. Allowed values: 1, 2, 3 |
| `amoc_kw` | `string` | `varchar` |  |  | Costs in WIP (keyword). Allowed values: tpppc.dwip.plus, tpppc.dwip.minus, tpppc.dwip.not |
| `expc` | `integer` | `int` |  |  | Commitments in WIP. Allowed values: 1, 2, 3 |
| `expc_kw` | `string` | `varchar` |  |  | Commitments in WIP (keyword). Allowed values: tpppc.dwip.plus, tpppc.dwip.minus, tpppc.dwip.not |
| `amos` | `integer` | `int` |  |  | Sales Value Surcharge Costs in WIP. Allowed values: 0, 1, 2, 3 |
| `amos_kw` | `string` | `varchar` |  |  | Sales Value Surcharge Costs in WIP (keyword). Allowed values: , tpppc.dwip.plus, tpppc.dwip.minus, tpppc.dwip.not |
| `amor` | `integer` | `int` |  |  | Revenues in WIP. Allowed values: 1, 2, 3 |
| `amor_kw` | `string` | `varchar` |  |  | Revenues in WIP (keyword). Allowed values: tpppc.dwip.plus, tpppc.dwip.minus, tpppc.dwip.not |
| `cprp` | `integer` | `int` |  |  | Interim Result in WIP. Allowed values: 1, 2, 3 |
| `cprp_kw` | `string` | `varchar` |  |  | Interim Result in WIP (keyword). Allowed values: tpppc.dwip.plus, tpppc.dwip.minus, tpppc.dwip.not |
| `ipiw` | `integer` | `int` |  |  | Project Inventory in Costs. Allowed values: 1, 2 |
| `ipiw_kw` | `string` | `varchar` |  |  | Project Inventory in Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prhr` | `integer` | `int` |  |  | Physical Progress Labor based on Hours. Allowed values: 0, 1, 2 |
| `prhr_kw` | `string` | `varchar` |  |  | Physical Progress Labor based on Hours (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `qacc` | `integer` | `int` |  |  | Convert to Unit of Control Code. Allowed values: 1, 2 |
| `qacc_kw` | `string` | `varchar` |  |  | Convert to Unit of Control Code (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cptc` | `string` | `varchar` |  |  | Code Period Table for Cost Control Period. Max length: 6 |
| `cpth` | `string` | `varchar` |  |  | Code Period Table for Hours Control Period. Max length: 6 |
| `tohc` | `integer` | `int` |  |  | Type of Hard Commitment. Allowed values: 1, 2 |
| `tohc_kw` | `string` | `varchar` |  |  | Type of Hard Commitment (keyword). Allowed values: tpppc.tohc.cost, tpppc.tohc.comm |
| `cmif` | `integer` | `int` |  |  | Use Commitments as Basis for Forecast. Allowed values: 1, 2 |
| `cmif_kw` | `string` | `varchar` |  |  | Use Commitments as Basis for Forecast (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `lfcm` | `integer` | `int` |  |  | Leading Forecast Method. Allowed values: 1, 2, 3 |
| `lfcm_kw` | `string` | `varchar` |  |  | Leading Forecast Method (keyword). Allowed values: tppdm.lfcm.est.extra.costs, tppdm.lfcm.est.to.compl, tppdm.lfcm.est.at.compl |
| `awpp` | `integer` | `int` |  |  | Extra WIP Entry in Finance. Allowed values: 1, 2 |
| `awpp_kw` | `string` | `varchar` |  |  | Extra WIP Entry in Finance (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `casd` | `string` | `varchar` |  |  | Additional Tax Info. Max length: 3 |
| `casc` | `string` | `varchar` |  |  | Additional Info Set. Max length: 3 |
| `cptf` | `string` | `varchar` |  |  | Code Period Table for Fiscal Period. Max length: 6 |
| `urcc` | `integer` | `int` |  |  | Cost Component for Cost-Plus Invoicing from Revenue. Allowed values: 1, 2 |
| `urcc_kw` | `string` | `varchar` |  |  | Cost Component for Cost-Plus Invoicing from Revenue (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `antp` | `integer` | `int` |  |  | Total Paid/Received Amount to Include Anticipated Amount. Allowed values: 0, 1, 2 |
| `antp_kw` | `string` | `varchar` |  |  | Total Paid/Received Amount to Include Anticipated Amount (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `tsem` | `string` | `varchar` |  |  | Default Employee. Max length: 9 |
| `ditm` | `string` | `varchar` |  |  | Default Item. Max length: 47 |
| `tstk` | `string` | `varchar` |  |  | Default Task. Max length: 8 |
| `dequ` | `string` | `varchar` |  |  | Default Equipment. Max length: 47 |
| `dsub` | `string` | `varchar` |  |  | Default Subcontracting. Max length: 47 |
| `dcic` | `string` | `varchar` |  |  | Default Sundry Cost. Max length: 8 |
| `dtrl` | `string` | `varchar` |  |  | Default Travel. Max length: 8 |
| `dfrt` | `string` | `varchar` |  |  | Default Freight. Max length: 8 |
| `dhlp` | `string` | `varchar` |  |  | Default Helpdesk. Max length: 8 |
| `ipcs` | `integer` | `int` |  |  | Include PCS WIP. Allowed values: 1, 2, 3 |
| `ipcs_kw` | `string` | `varchar` |  |  | Include PCS WIP (keyword). Allowed values: tpppc.ipcs.inhc, tpppc.ipcs.insc, tpppc.ipcs.not |
| `revr` | `integer` | `int` |  |  | Post Interim Results. Allowed values: 10, 20, 30 |
| `revr_kw` | `string` | `varchar` |  |  | Post Interim Results (keyword). Allowed values: tpppc.rev.rec.interim.result, tpppc.rev.rec.balance, tpppc.rev.rec.interim.balance |
| `cint` | `integer` | `int` |  |  | Post Completion Transactions Internal Project. Allowed values: 10, 20, 30 |
| `cint_kw` | `string` | `varchar` |  |  | Post Completion Transactions Internal Project (keyword). Allowed values: tpppc.cint.direct, tpppc.cint.close.structure, tpppc.cint.close.project |
| `ccap` | `integer` | `int` |  |  | Post Completion Transactions Capital Project. Allowed values: 10, 20 |
| `ccap_kw` | `string` | `varchar` |  |  | Post Completion Transactions Capital Project (keyword). Allowed values: tpppc.ccap.close.structure, tpppc.ccap.close.project |
| `ppht` | `integer` | `int` |  |  | Post Project Hours Transactions. Allowed values: 10, 20 |
| `ppht_kw` | `string` | `varchar` |  |  | Post Project Hours Transactions (keyword). Allowed values: tpppc.ppht.by.calendar.day, tpppc.ppht.aggregated |
| `mfcr` | `integer` | `int` |  |  | Multi Financial Company Project. Allowed values: 1, 2 |
| `mfcr_kw` | `string` | `varchar` |  |  | Multi Financial Company Project (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scev` | `integer` | `int` |  |  | Include Soft Commitments in Earned Value. Allowed values: 1, 2 |
| `scev_kw` | `string` | `varchar` |  |  | Include Soft Commitments in Earned Value (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `upsa` | `integer` | `int` |  |  | Use Progress Setting for all Projects. Allowed values: 1, 2 |
| `upsa_kw` | `string` | `varchar` |  |  | Use Progress Setting for all Projects (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `text` | `integer` | `int` |  |  | Text |
| `mcpr` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `mcpr_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cpth_hyea_hper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cpth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `casd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `casc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `cptf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `tsem_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
| `ditm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm005 Item Project Data |
| `tstk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm015 Labor |
| `dequ_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm025 Equipment |
| `dsub_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm035 Subcontracting |
| `dcic_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm040 Sundry Costs |
| `dtrl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm040 Sundry Costs |
| `dfrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm040 Sundry Costs |
| `dhlp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm040 Sundry Costs |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

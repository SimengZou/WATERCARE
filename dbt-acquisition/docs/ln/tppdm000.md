# tppdm000

LN tppdm000 General Project Parameters table, Generated 2026-04-10T19:41:52Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm000` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt` |
| **Column count** | 159 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Introduction Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `uset` | `string` | `varchar` |  |  | Unit Set. Max length: 6 |
| `ctut` | `string` | `varchar` |  |  | Unit Code for Hours. Max length: 3 |
| `ccpr` | `integer` | `int` |  |  | Project. Allowed values: 1, 2 |
| `ccpr_kw` | `string` | `varchar` |  |  | Project (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cprt` | `integer` | `int` |  |  | Project/Cost Type. Allowed values: 1, 2 |
| `cprt_kw` | `string` | `varchar` |  |  | Project/Cost Type (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cprc` | `integer` | `int` |  |  | Project/Cost Component. Allowed values: 1, 2 |
| `cprc_kw` | `string` | `varchar` |  |  | Project/Cost Component (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cpru` | `integer` | `int` |  |  | Project/Control Code. Allowed values: 1, 2 |
| `cpru_kw` | `string` | `varchar` |  |  | Project/Control Code (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cpsp` | `integer` | `int` |  |  | Project/Element. Allowed values: 1, 2 |
| `cpsp_kw` | `string` | `varchar` |  |  | Project/Element (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cspt` | `integer` | `int` |  |  | Project/Element/Cost Type. Allowed values: 1, 2 |
| `cspt_kw` | `string` | `varchar` |  |  | Project/Element/Cost Type (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cspc` | `integer` | `int` |  |  | Project/Element/Cost Component. Allowed values: 1, 2 |
| `cspc_kw` | `string` | `varchar` |  |  | Project/Element/Cost Component (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cspu` | `integer` | `int` |  |  | Project/Element/Control Code. Allowed values: 1, 2 |
| `cspu_kw` | `string` | `varchar` |  |  | Project/Element/Control Code (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cpac` | `integer` | `int` |  |  | Project/Activity. Allowed values: 1, 2 |
| `cpac_kw` | `string` | `varchar` |  |  | Project/Activity (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cact` | `integer` | `int` |  |  | Project/Activity/Cost Type. Allowed values: 1, 2 |
| `cact_kw` | `string` | `varchar` |  |  | Project/Activity/Cost Type (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cacc` | `integer` | `int` |  |  | Project/Activity/Cost Component. Allowed values: 1, 2 |
| `cacc_kw` | `string` | `varchar` |  |  | Project/Activity/Cost Component (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cacu` | `integer` | `int` |  |  | Project/Activity/Cost Control Code. Allowed values: 1, 2 |
| `cacu_kw` | `string` | `varchar` |  |  | Project/Activity/Cost Control Code (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cpst` | `integer` | `int` |  |  | Project/Extension. Allowed values: 1, 2 |
| `cpst_kw` | `string` | `varchar` |  |  | Project/Extension (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstt` | `integer` | `int` |  |  | Project/Extension/Cost Type. Allowed values: 1, 2 |
| `cstt_kw` | `string` | `varchar` |  |  | Project/Extension/Cost Type (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstc` | `integer` | `int` |  |  | Project/Extension/Cost Component. Allowed values: 1, 2 |
| `cstc_kw` | `string` | `varchar` |  |  | Project/Extension/Cost Component (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstu` | `integer` | `int` |  |  | Project/Extension/Control Code. Allowed values: 1, 2 |
| `cstu_kw` | `string` | `varchar` |  |  | Project/Extension/Control Code (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `uwka` | `integer` | `int` |  |  | Use WKA (Dutch law regarding tax payments). Allowed values: 1, 2 |
| `uwka_kw` | `string` | `varchar` |  |  | Use WKA (Dutch law regarding tax payments) (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `elus` | `integer` | `int` |  |  | Use Element. Allowed values: 1, 2 |
| `elus_kw` | `string` | `varchar` |  |  | Use Element (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acus` | `integer` | `int` |  |  | Use Activity. Allowed values: 1, 2 |
| `acus_kw` | `string` | `varchar` |  |  | Use Activity (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `exus` | `integer` | `int` |  |  | Use Extension. Allowed values: 1, 2 |
| `exus_kw` | `string` | `varchar` |  |  | Use Extension (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccus` | `integer` | `int` |  |  | Use Cost Component. Allowed values: 1, 2 |
| `ccus_kw` | `string` | `varchar` |  |  | Use Cost Component (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dpci` | `string` | `varchar` |  |  | Revenue Code for Installment. Max length: 8 |
| `dpca` | `string` | `varchar` |  |  | Revenue Code for Additional Work. Max length: 8 |
| `dpch` | `string` | `varchar` |  |  | Revenue Code for Holdback. Max length: 8 |
| `rcpp` | `string` | `varchar` |  |  | Revenue Code for Progress Payment. Max length: 8 |
| `rcfe` | `string` | `varchar` |  |  | Revenue Code for Fee. Max length: 8 |
| `rcpt` | `string` | `varchar` |  |  | Revenue Code for Penalty. Max length: 8 |
| `rcap` | `string` | `varchar` |  |  | Revenue Code for Advance Payment. Max length: 8 |
| `frvt` | `float` | `float` |  |  | Fee Revenue Threshold |
| `pcsp` | `integer` | `int` |  |  | Process Subprojects with Main Project. Allowed values: 1, 2 |
| `pcsp_kw` | `string` | `varchar` |  |  | Process Subprojects with Main Project (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `pcmp` | `integer` | `int` |  |  | Aggregate Main Project from Subprojects. Allowed values: 1, 2 |
| `pcmp_kw` | `string` | `varchar` |  |  | Aggregate Main Project from Subprojects (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `lexp` | `integer` | `int` |  |  | Log Commitments (Purchase Transactions). Allowed values: 1, 2, 3 |
| `lexp_kw` | `string` | `varchar` |  |  | Log Commitments (Purchase Transactions) (keyword). Allowed values: tppdm.lexp.not, tppdm.lexp.both, tppdm.lexp.pur.deliv |
| `lccc` | `integer` | `int` |  |  | Log Commitments (Customer Claims). Allowed values: 1, 2 |
| `lccc_kw` | `string` | `varchar` |  |  | Log Commitments (Customer Claims) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `citt` | `string` | `varchar` |  |  | Item Code System (File Import). Max length: 3 |
| `rscs` | `string` | `varchar` |  |  | Default Extension Code for Fluctuation Settlement. Max length: 4 |
| `capp` | `string` | `varchar` |  |  | Appointment Type. Max length: 3 |
| `cgch` | `integer` | `int` |  |  | Copy General Surcharges when creating Project. Allowed values: 1, 2, 3, 4 |
| `cgch_kw` | `string` | `varchar` |  |  | Copy General Surcharges when creating Project (keyword). Allowed values: tppdm.cgch.not, tppdm.cgch.tech.calc, tppdm.cgch.cost.reg, tppdm.cgch.both |
| `upra` | `integer` | `int` |  |  | Project Archives. Allowed values: 1, 2 |
| `upra_kw` | `string` | `varchar` |  |  | Project Archives (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rtyp` | `string` | `varchar` |  |  | Default Exchange Rate Type Budget. Max length: 3 |
| `rrtp` | `string` | `varchar` |  |  | Default Exchange Rate Type for Revenues. Max length: 3 |
| `crtp` | `string` | `varchar` |  |  | Default Exchange Rate Type for Costs. Max length: 3 |
| `usev` | `integer` | `int` |  |  | All Activity Types. Allowed values: 1, 2 |
| `usev_kw` | `string` | `varchar` |  |  | All Activity Types (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `serr` | `string` | `varchar` |  |  | Order Series for PCS Project. Max length: 8 |
| `tpte` | `string` | `varchar` |  |  | Default Template. Max length: 9 |
| `ngrp` | `string` | `varchar` |  |  | Project Number Group. Max length: 3 |
| `prsr` | `string` | `varchar` |  |  | Project Series. Max length: 8 |
| `ests` | `string` | `varchar` |  |  | Estimate Series. Max length: 8 |
| `egrp` | `string` | `varchar` |  |  | Estimate Line Number Group. Max length: 3 |
| `essr` | `string` | `varchar` |  |  | Estimate Line Series. Max length: 8 |
| `nlit` | `integer` | `int` |  |  | Sequence No. Length Item Codes |
| `nlta` | `integer` | `int` |  |  | Sequence No. Length Labor Codes |
| `nleq` | `integer` | `int` |  |  | Sequence No. Length Equipment Codes |
| `nlci` | `integer` | `int` |  |  | Sequence No. Length Sundry Cost Codes |
| `tgrp` | `string` | `varchar` |  |  | Template Number Group. Max length: 3 |
| `tpsr` | `string` | `varchar` |  |  | Template Series. Max length: 8 |
| `nlsb` | `integer` | `int` |  |  | Sequence No. Length Subcontracting Codes |
| `prbs` | `integer` | `int` |  |  | Profit Base. Allowed values: 1, 2 |
| `prbs_kw` | `string` | `varchar` |  |  | Profit Base (keyword). Allowed values: tpest.prbs.cost, tpest.prbs.sales |
| `pwar` | `string` | `varchar` |  |  | Priority Supply Warehouse. Max length: 6 |
| `ohac` | `integer` | `int` |  |  | Overhead Actuals. Allowed values: 1, 2 |
| `ohac_kw` | `string` | `varchar` |  |  | Overhead Actuals (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ohhc` | `integer` | `int` |  |  | Overhead Hard Commitments. Allowed values: 1, 2 |
| `ohhc_kw` | `string` | `varchar` |  |  | Overhead Hard Commitments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ohim` | `integer` | `int` |  |  | Overhead Include in Monitoring. Allowed values: 1, 2 |
| `ohim_kw` | `string` | `varchar` |  |  | Overhead Include in Monitoring (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `citg` | `string` | `varchar` |  |  | Item Code System (Order Entry). Max length: 3 |
| `prhs` | `integer` | `int` |  |  | Log Project History. Allowed values: 1, 2 |
| `prhs_kw` | `string` | `varchar` |  |  | Log Project History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `achs` | `integer` | `int` |  |  | Log Activity History. Allowed values: 1, 2 |
| `achs_kw` | `string` | `varchar` |  |  | Log Activity History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elhs` | `integer` | `int` |  |  | Log Element History. Allowed values: 1, 2 |
| `elhs_kw` | `string` | `varchar` |  |  | Log Element History (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bghi` | `integer` | `int` |  |  | Hide Project Bank Guarantee. Allowed values: 1, 2 |
| `bghi_kw` | `string` | `varchar` |  |  | Hide Project Bank Guarantee (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgnw` | `integer` | `int` |  |  | Allow Project Bank Guarantee for new Documents. Allowed values: 1, 2 |
| `bgnw_kw` | `string` | `varchar` |  |  | Allow Project Bank Guarantee for new Documents (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `plmi` | `integer` | `int` |  |  | PLM Integration. Allowed values: 1, 2 |
| `plmi_kw` | `string` | `varchar` |  |  | PLM Integration (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `plmc` | `integer` | `int` |  |  | PLM Company |
| `nbpt` | `integer` | `int` |  |  | Use New PLM Business Process Type. Allowed values: 1, 2 |
| `nbpt_kw` | `string` | `varchar` |  |  | Use New PLM Business Process Type (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rtus` | `integer` | `int` |  |  | Equipment Rental. Allowed values: 1, 2 |
| `rtus_kw` | `string` | `varchar` |  |  | Equipment Rental (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `text` | `integer` | `int` |  |  | Text |
| `cdf_acnl` | `integer` | `int` |  |  | Parent/Child activity control. Allowed values: 1, 2 |
| `cdf_acnl_kw` | `string` | `varchar` |  |  | Parent/Child activity control (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdf_ocrl` | `integer` | `int` |  |  | Opex Control. Allowed values: 1, 2 |
| `cdf_ocrl_kw` | `string` | `varchar` |  |  | Opex Control (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdf_scrl` | `integer` | `int` |  |  | Spend Control. Allowed values: 1, 2 |
| `cdf_scrl_kw` | `string` | `varchar` |  |  | Spend Control (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdf_sres` | `string` | `varchar` |  |  | Responsibility. Max length: 3 |
| `uset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs006 Unit Sets |
| `ctut_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `dpci_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm043 Revenues |
| `dpca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm043 Revenues |
| `dpch_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm043 Revenues |
| `rcpp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm043 Revenues |
| `rcfe_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm043 Revenues |
| `rcpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm043 Revenues |
| `rcap_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm043 Revenues |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `capp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm053 Appointment Types |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `rrtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `crtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `tpte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ngrp_prsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngrp_ests_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `egrp_essr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `tgrp_tpsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `tgrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `pwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `citg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `plmc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cdf_sres_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm048 Responsibilities |
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

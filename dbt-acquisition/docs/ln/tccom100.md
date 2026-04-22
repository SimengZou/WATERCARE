# tccom100

LN tccom100 Business Partners table, Generated 2026-04-10T19:41:00Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `bpid` |
| **Column count** | 112 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `bpid` | `string` | `varchar` | `PK` | `not_null` | (required) Business Partner. Max length: 9 |
| `ctit` | `string` | `varchar` |  |  | Title. Max length: 3 |
| `nama__en_us` | `string` | `varchar` |  | `not_null` | (required) Name - base language. Max length: 35 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `prbp` | `string` | `varchar` |  |  | Parent Business Partner. Max length: 9 |
| `prst` | `integer` | `int` |  |  | Business Partner Status. Allowed values: 1, 2, 3, 254 |
| `prst_kw` | `string` | `varchar` |  |  | Business Partner Status (keyword). Allowed values: tccom.prst.potential, tccom.prst.active, tccom.prst.inactive, tccom.prst.not.specified |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `sndr` | `integer` | `int` |  |  | One-Time Business Partner. Allowed values: 1, 2 |
| `sndr_kw` | `string` | `varchar` |  |  | One-Time Business Partner (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `edyn` | `integer` | `int` |  |  | EDI. Allowed values: 1, 2 |
| `edyn_kw` | `string` | `varchar` |  |  | EDI (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fovn` | `string` | `varchar` |  |  | Obsolete. Max length: 35 |
| `lvdt` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `inrl` | `integer` | `int` |  |  | Affiliated Company. Allowed values: 1, 2 |
| `inrl_kw` | `string` | `varchar` |  |  | Affiliated Company (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iscn` | `integer` | `int` |  |  | Affiliated Company No. |
| `lgid__en_us` | `string` | `varchar` |  | `not_null` | (required) Chamber of Commerce - base language. Max length: 20 |
| `cmid__en_us` | `string` | `varchar` |  | `not_null` | (required) Commercial Identification - base language. Max length: 20 |
| `bptx` | `string` | `varchar` |  |  | Business Partner for Texts. Max length: 9 |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `telp` | `string` | `varchar` |  |  | Phone Number. Max length: 32 |
| `tlcy` | `string` | `varchar` |  |  | Country for Phone Number. Max length: 6 |
| `tlci` | `string` | `varchar` |  |  | City/Area Code for Phone Number. Max length: 15 |
| `tlln` | `string` | `varchar` |  |  | Local Number for Phone Number. Max length: 32 |
| `tlex` | `string` | `varchar` |  |  | Extension for Phone Number. Max length: 15 |
| `tefx` | `string` | `varchar` |  |  | Fax Number. Max length: 32 |
| `tfcy` | `string` | `varchar` |  |  | Country for Fax Number. Max length: 6 |
| `tfci` | `string` | `varchar` |  |  | City/Area Code for Fax Number. Max length: 15 |
| `tfln` | `string` | `varchar` |  |  | Local Number for Fax Number. Max length: 32 |
| `tfex` | `string` | `varchar` |  |  | Extension for Fax Number. Max length: 15 |
| `ccnt` | `string` | `varchar` |  |  | Primary Contact. Max length: 9 |
| `cint` | `integer` | `int` |  |  | Internal Business Partner. Allowed values: 1, 2 |
| `cint_kw` | `string` | `varchar` |  |  | Internal Business Partner (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `foid` | `integer` | `int` |  |  | Front Office ID |
| `crid` | `integer` | `int` |  |  | Creator's Unique System ID |
| `icst` | `integer` | `int` |  |  | Intercompany Settlement. Allowed values: 1, 2 |
| `icst_kw` | `string` | `varchar` |  |  | Intercompany Settlement (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `icod` | `integer` | `int` |  |  | I-Code. Allowed values: 1, 2, 3, 4, 5, 6, 7, 254 |
| `icod_kw` | `string` | `varchar` |  |  | I-Code (keyword). Allowed values: tcicod.goods, tcicod.triangle.trade, tcicod.work.order, tcicod.none, tcicod.exempt.suppl, tcicod.consignm.invd, tcicod.assets, tcicod.not.specified |
| `fact` | `integer` | `int` |  |  | Factor. Allowed values: 1, 2 |
| `fact_kw` | `string` | `varchar` |  |  | Factor (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ecmp` | `integer` | `int` |  |  | Enterprise Unit Company |
| `coff` | `integer` | `int` |  |  | Collection Office. Allowed values: 1, 2 |
| `coff_kw` | `string` | `varchar` |  |  | Collection Office (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `csto` | `integer` | `int` |  |  | Customs Office. Allowed values: 1, 2 |
| `csto_kw` | `string` | `varchar` |  |  | Customs Office (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `bprl` | `integer` | `int` |  |  | Business Partner Role. Allowed values: 1, 2, 3, 4 |
| `bprl_kw` | `string` | `varchar` |  |  | Business Partner Role (keyword). Allowed values: tcbprl.none, tcbprl.customer, tcbprl.supplier, tcbprl.both |
| `btbv` | `integer` | `int` |  |  | To be Verified. Allowed values: 1, 2 |
| `btbv_kw` | `string` | `varchar` |  |  | To be Verified (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `usid` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `clcd` | `integer` | `int` |  |  | Credit Limit Check Per Department. Allowed values: 1, 2 |
| `clcd_kw` | `string` | `varchar` |  |  | Credit Limit Check Per Department (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cbcl` | `string` | `varchar` |  |  | Currency for BP Balances/Credit Limits. Max length: 3 |
| `beid__en_us` | `string` | `varchar` |  | `not_null` | (required) Business Entity Identifier - base language. Max length: 15 |
| `inet__en_us` | `string` | `varchar` |  | `not_null` | (required) Website - base language. Max length: 150 |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `smt1` | `integer` | `int` |  |  | Social Media Type 1. Allowed values: 1, 2, 5, 10, 15, 20, 25, 99, 254 |
| `smt1_kw` | `string` | `varchar` |  |  | Social Media Type 1 (keyword). Allowed values: tccom.smty.twitter, tccom.smty.twitter.tags, tccom.smty.facebook, tccom.smty.linkedin, tccom.smty.myspace, tccom.smty.hyves, tccom.smty.yammer, tccom.smty.other, tccom.smty.not.specified |
| `sml1__en_us` | `string` | `varchar` |  | `not_null` | (required) Social Media Link 1 - base language. Max length: 150 |
| `smt2` | `integer` | `int` |  |  | Social Media Type 2. Allowed values: 1, 2, 5, 10, 15, 20, 25, 99, 254 |
| `smt2_kw` | `string` | `varchar` |  |  | Social Media Type 2 (keyword). Allowed values: tccom.smty.twitter, tccom.smty.twitter.tags, tccom.smty.facebook, tccom.smty.linkedin, tccom.smty.myspace, tccom.smty.hyves, tccom.smty.yammer, tccom.smty.other, tccom.smty.not.specified |
| `sml2__en_us` | `string` | `varchar` |  | `not_null` | (required) Social Media Link 2 - base language. Max length: 150 |
| `smt3` | `integer` | `int` |  |  | Social Media Type 3. Allowed values: 1, 2, 5, 10, 15, 20, 25, 99, 254 |
| `smt3_kw` | `string` | `varchar` |  |  | Social Media Type 3 (keyword). Allowed values: tccom.smty.twitter, tccom.smty.twitter.tags, tccom.smty.facebook, tccom.smty.linkedin, tccom.smty.myspace, tccom.smty.hyves, tccom.smty.yammer, tccom.smty.other, tccom.smty.not.specified |
| `sml3__en_us` | `string` | `varchar` |  | `not_null` | (required) Social Media Link 3 - base language. Max length: 150 |
| `smt4` | `integer` | `int` |  |  | Social Media Type 4. Allowed values: 1, 2, 5, 10, 15, 20, 25, 99, 254 |
| `smt4_kw` | `string` | `varchar` |  |  | Social Media Type 4 (keyword). Allowed values: tccom.smty.twitter, tccom.smty.twitter.tags, tccom.smty.facebook, tccom.smty.linkedin, tccom.smty.myspace, tccom.smty.hyves, tccom.smty.yammer, tccom.smty.other, tccom.smty.not.specified |
| `sml4__en_us` | `string` | `varchar` |  | `not_null` | (required) Social Media Link 4 - base language. Max length: 150 |
| `smt5` | `integer` | `int` |  |  | Social Media Type 5. Allowed values: 1, 2, 5, 10, 15, 20, 25, 99, 254 |
| `smt5_kw` | `string` | `varchar` |  |  | Social Media Type 5 (keyword). Allowed values: tccom.smty.twitter, tccom.smty.twitter.tags, tccom.smty.facebook, tccom.smty.linkedin, tccom.smty.myspace, tccom.smty.hyves, tccom.smty.yammer, tccom.smty.other, tccom.smty.not.specified |
| `sml5__en_us` | `string` | `varchar` |  | `not_null` | (required) Social Media Link 5 - base language. Max length: 150 |
| `txta` | `integer` | `int` |  |  | Text |
| `cdf_bsrm` | `integer` | `int` |  |  | SRM. Allowed values: 1, 2, 3, 4 |
| `cdf_bsrm_kw` | `string` | `varchar` |  |  | SRM (keyword). Allowed values: txcom.srm.high, txcom.srm.medium, txcom.srm.low, txcom.srm.no |
| `cdf_cond` | `timestamp` | `timestamp_ntz` |  |  | Code of Conduct Date |
| `cdf_crtl` | `integer` | `int` |  |  | Critical. Allowed values: 1, 2 |
| `cdf_crtl_kw` | `string` | `varchar` |  |  | Critical (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdf_emno` | `integer` | `int` |  |  | Employee. Allowed values: 1, 2 |
| `cdf_emno_kw` | `string` | `varchar` |  |  | Employee (keyword). Allowed values: tccdf______chk.yes, tccdf______chk.no |
| `cdf_exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `cdf_hsst` | `integer` | `int` |  |  | H&S PreQual. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_hsst_kw` | `string` | `varchar` |  |  | H&S PreQual (keyword). Allowed values: txstatus.cat1, txstatus.cat2, txstatus.cat3, txstatus.cats, txstatus.intl, txstatus.nope |
| `cdf_rrdt` | `timestamp` | `timestamp_ntz` |  |  | Recall Requested Time |
| `cdf_wfst` | `integer` | `int` |  |  | Workflow Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_wfst_kw` | `string` | `varchar` |  |  | Workflow Status (keyword). Allowed values: tccdf_lst003.not.applicable, tccdf_lst003.approved, tccdf_lst003.rejected, tccdf_lst003.pending, tccdf_lst003.recall, tccdf_lst003.appr.received |
| `ctit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs019 Titles |
| `prbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `bptx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cbcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

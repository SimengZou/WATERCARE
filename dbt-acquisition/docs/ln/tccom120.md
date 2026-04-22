# tccom120

LN tccom120 Buy-from Business Partners table, Generated 2026-04-10T19:41:02Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom120` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `otbp` |
| **Column count** | 125 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `otbp` | `string` | `varchar` | `PK` | `not_null` | (required) Buy-from Business Partner. Max length: 9 |
| `bpst` | `integer` | `int` |  |  | Business Partner Status. Allowed values: 1, 2, 3, 254 |
| `bpst_kw` | `string` | `varchar` |  |  | Business Partner Status (keyword). Allowed values: tccom.prst.potential, tccom.prst.active, tccom.prst.inactive, tccom.prst.not.specified |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `creg` | `string` | `varchar` |  |  | Area. Max length: 3 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `cbps` | `string` | `varchar` |  |  | Business Partner Signal. Max length: 3 |
| `cbtp` | `string` | `varchar` |  |  | Business Partner Type. Max length: 3 |
| `vryn` | `integer` | `int` |  |  | Vendor Rating. Allowed values: 1, 2 |
| `vryn_kw` | `string` | `varchar` |  |  | Vendor Rating (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpus` | `string` | `varchar` |  |  | Business Partner for Statistics Update. Max length: 9 |
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
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `ccnt` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `mcfr` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `mcfr_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `raur` | `integer` | `int` |  |  | Use Purchase Rates for Receipts. Allowed values: 1, 2 |
| `raur_kw` | `string` | `varchar` |  |  | Use Purchase Rates for Receipts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cplp` | `string` | `varchar` |  |  | Purchase Price List. Max length: 3 |
| `odis` | `float` | `float` |  |  | Order Discount |
| `bppr` | `string` | `varchar` |  |  | Business Partner for Prices/Discounts. Max length: 9 |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `potp` | `string` | `varchar` |  |  | Purchase Order Type. Max length: 3 |
| `ocus` | `string` | `varchar` |  |  | Our Customer Number. Max length: 15 |
| `clgr` | `string` | `varchar` |  |  | List Group. Max length: 3 |
| `ackx` | `integer` | `int` |  |  | Print Purchase Order by Exception. Allowed values: 1, 2 |
| `ackx_kw` | `string` | `varchar` |  |  | Print Purchase Order by Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sbil` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbil_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `paft` | `integer` | `int` |  |  | Invoice After. Allowed values: 1, 2 |
| `paft_kw` | `string` | `varchar` |  |  | Invoice After (keyword). Allowed values: tcpaft.approval, tcpaft.receipts |
| `sbdt` | `integer` | `int` |  |  | Self-Billing Date Type. Allowed values: 10, 20 |
| `sbdt_kw` | `string` | `varchar` |  |  | Self-Billing Date Type (keyword). Allowed values: tcsbdt.receipt.date, tcsbdt.shipping.date |
| `mesv` | `integer` | `int` |  |  | Generate Release per Vehicle. Allowed values: 1, 2 |
| `mesv_kw` | `string` | `varchar` |  |  | Generate Release per Vehicle (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `encd` | `integer` | `int` |  |  | Environmental Compliance Declared. Allowed values: 1, 2 |
| `encd_kw` | `string` | `varchar` |  |  | Environmental Compliance Declared (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crrf` | `integer` | `int` |  |  | Item Cross Reference. Allowed values: 1, 2, 3 |
| `crrf_kw` | `string` | `varchar` |  |  | Item Cross Reference (keyword). Allowed values: tccrrf.ics, tccrrf.mpn, tccrrf.na |
| `ucnd` | `integer` | `int` |  |  | Use Confirmation. Allowed values: 1, 2 |
| `ucnd_kw` | `string` | `varchar` |  |  | Use Confirmation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `rdec` | `string` | `varchar` |  |  | Return Delivery Terms. Max length: 3 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `scqq` | `integer` | `int` |  |  | Schedule Quantity Qualifier. Allowed values: 1, 2, 254 |
| `scqq_kw` | `string` | `varchar` |  |  | Schedule Quantity Qualifier (keyword). Allowed values: tccom.scqq.cum, tccom.scqq.ran, tccom.scqq.not.specified |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `frin` | `integer` | `int` |  |  | Invoice Business Partner for Freight. Allowed values: 1, 2 |
| `frin_kw` | `string` | `varchar` |  |  | Invoice Business Partner for Freight (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `afrb` | `integer` | `int` |  |  | Retro-Billing Applicable. Allowed values: 1, 2 |
| `afrb_kw` | `string` | `varchar` |  |  | Retro-Billing Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sbvf` | `timestamp` | `timestamp_ntz` |  |  | Self-Billing Validity Date-from |
| `sbvt` | `timestamp` | `timestamp_ntz` |  |  | Self-Billing Validity Date-to |
| `sbrf` | `string` | `varchar` |  |  | Self-Billing Reference. Max length: 9 |
| `tira` | `integer` | `int` |  |  | Tax Invoice Required for AFP. Allowed values: 1, 2 |
| `tira_kw` | `string` | `varchar` |  |  | Tax Invoice Required for AFP (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nasu` | `string` | `varchar` |  |  | Nature of Supply. Max length: 9 |
| `srtr` | `string` | `varchar` |  |  | Services Trade. Max length: 9 |
| `dset` | `string` | `varchar` |  |  | Document Set. Max length: 9 |
| `dlid` | `string` | `varchar` |  |  | Document Line Id. Max length: 22 |
| `retp` | `float` | `float` |  |  | Retention % |
| `asbs` | `integer` | `int` |  |  | Assessment Basis. Allowed values: 5, 10, 90 |
| `asbs_kw` | `string` | `varchar` |  |  | Assessment Basis (keyword). Allowed values: tcspd.asbs.period, tcspd.asbs.cumul, tcspd.asbs.not.appl |
| `dlpe` | `integer` | `int` |  |  | Defects Liability Period |
| `dlpu` | `integer` | `int` |  |  | Defects Liability Period Unit. Allowed values: 10, 20, 30, 40, 50, 90 |
| `dlpu_kw` | `string` | `varchar` |  |  | Defects Liability Period Unit (keyword). Allowed values: tctmun.hours, tctmun.days, tctmun.weeks, tctmun.months, tctmun.years, tctmun.not.applicable |
| `sigr` | `string` | `varchar` |  |  | Serialized Item Group. Max length: 6 |
| `itgr` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `spot` | `string` | `varchar` |  |  | Services Procurement Order Type. Max length: 3 |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `txta` | `integer` | `int` |  |  | Text |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `cbps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs039 Signals |
| `cbtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs029 Business Partner Types |
| `bpus_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cplp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `bppr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `clgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd301 List Groups |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `rdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `nasu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs030 Natures of Supply |
| `srtr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs213 Services Trades |
| `dset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcbcm021 Document Sets |
| `itgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs023 Item Groups |
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

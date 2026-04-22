# tpppc250

LN tpppc250 Commitments (Equipment) table, Generated 2026-04-10T19:42:09Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc250` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cequ`, `sern` |
| **Column count** | 73 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cequ` | `string` | `varchar` | `PK` | `not_null` | (required) Equipment. Max length: 47 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `time` | `float` | `float` |  |  | Amount of Time |
| `cuti` | `string` | `varchar` |  |  | Time Unit. Max length: 3 |
| `quan` | `float` | `float` |  |  | Quantity |
| `ratc` | `float` | `float` |  |  | Cost Rate |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rtcc_1` | `float` | `float` |  |  | Rate |
| `rtcc_2` | `float` | `float` |  |  | Rate |
| `rtcc_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `cdoc__en_us` | `string` | `varchar` |  | `not_null` | (required) Document - base language. Max length: 10 |
| `cptc` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `year` | `integer` | `int` |  |  | Cost Control Year |
| `peri` | `integer` | `int` |  |  | Cost Control Period |
| `cptf` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `fyea` | `integer` | `int` |  |  | Fiscal Year |
| `fper` | `integer` | `int` |  |  | Fiscal Period |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `orno` | `string` | `varchar` |  |  | Purchase Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Position Number |
| `srnb` | `integer` | `int` |  |  | Receipt Number |
| `cfpo` | `integer` | `int` |  |  | Approved for Posting. Allowed values: 1, 2 |
| `cfpo_kw` | `string` | `varchar` |  |  | Approved for Posting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `potf` | `integer` | `int` |  |  | Post to LN Financials. Allowed values: 1, 2 |
| `potf_kw` | `string` | `varchar` |  |  | Post to LN Financials (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `teta` | `integer` | `int` |  |  | Posting Type. Allowed values: 1, 3, 4, 5, 7, 8 |
| `teta_kw` | `string` | `varchar` |  |  | Posting Type (keyword). Allowed values: tpppc.teta.manu, tpppc.teta.order, tpppc.teta.deliver.tp, tpppc.teta.manu.rev, tpppc.teta.deliver.wh, tpppc.teta.deliver.tp.rev |
| `entu` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `loco` | `string` | `varchar` |  |  | User. Max length: 16 |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Fixed Currency Rate. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Fixed Currency Rate (keyword). Allowed values: , tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cuti_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cdoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin105 Invoice Text by Document |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cptf_fyea_fper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
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

# tpppc291

LN tpppc291 Sundry Costs table, Generated 2026-04-10T19:42:11Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc291` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cico`, `sern` |
| **Column count** | 109 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cico` | `string` | `varchar` | `PK` | `not_null` | (required) Sundry Cost. Max length: 8 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `quan` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `rtcc_1` | `float` | `float` |  |  | Currency Rate (Costs - Debit) |
| `rtcc_2` | `float` | `float` |  |  | Currency Rate (Costs - Debit) |
| `rtcc_3` | `float` | `float` |  |  | Currency Rate (Costs - Debit) |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor (Costs - Debit) |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor (Costs - Debit) |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor (Costs - Debit) |
| `cohd_1` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `cohd_2` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `cohd_3` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `curc` | `string` | `varchar` |  |  | Cost Currency. Max length: 3 |
| `pris` | `float` | `float` |  |  | Sales Price |
| `amos` | `float` | `float` |  |  | Sales Amount |
| `rtcs_1` | `float` | `float` |  |  | Currency Rate (Sales) |
| `rtcs_2` | `float` | `float` |  |  | Currency Rate (Sales) |
| `rtcs_3` | `float` | `float` |  |  | Currency Rate (Sales) |
| `rtfs_1` | `integer` | `int` |  |  | Rate Factor Sales |
| `rtfs_2` | `integer` | `int` |  |  | Rate Factor Sales |
| `rtfs_3` | `integer` | `int` |  |  | Rate Factor Sales |
| `curs` | `string` | `varchar` |  |  | Sales Currency. Max length: 3 |
| `cdoc__en_us` | `string` | `varchar` |  | `not_null` | (required) Document - base language. Max length: 10 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `cptc` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `year` | `integer` | `int` |  |  | Cost Control Year |
| `peri` | `integer` | `int` |  |  | Cost Control Period |
| `cptf` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `fyea` | `integer` | `int` |  |  | Fiscal Year |
| `fper` | `integer` | `int` |  |  | Fiscal Period |
| `ryea` | `integer` | `int` |  |  | Reporting Year |
| `rper` | `integer` | `int` |  |  | Reporting Period |
| `cfpo` | `integer` | `int` |  |  | Approved for Posting. Allowed values: 1, 2 |
| `cfpo_kw` | `string` | `varchar` |  |  | Approved for Posting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `potf` | `integer` | `int` |  |  | Post to LN Financials. Allowed values: 1, 2 |
| `potf_kw` | `string` | `varchar` |  |  | Post to LN Financials (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `teti` | `integer` | `int` |  |  | Posting Type. Allowed values: 1, 2, 3, 4, 5, 6 |
| `teti_kw` | `string` | `varchar` |  |  | Posting Type (keyword). Allowed values: tpppc.teti.manu, tpppc.teti.invoice, tpppc.teti.central.hours, tpppc.teti.charge.potf, tpppc.teti.charge.not.potf, tpppc.teti.service |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `loco` | `string` | `varchar` |  |  | User. Max length: 16 |
| `ncmp` | `integer` | `int` |  |  | Company Financial Document |
| `entu` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `ftty` | `string` | `varchar` |  |  | Transaction Type Financial Transaction. Max length: 3 |
| `fdoc` | `integer` | `int` |  |  | Document Financial Transaction |
| `flin` | `integer` | `int` |  |  | Line Number Financial Transaction |
| `fsrl` | `integer` | `int` |  |  | Sequence Number Finance |
| `ftln` | `integer` | `int` |  |  | Target Line Number |
| `sttl` | `integer` | `int` |  |  | Settlement Origin. Allowed values: 1, 2 |
| `sttl_kw` | `string` | `varchar` |  |  | Settlement Origin (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `hyea` | `integer` | `int` |  |  | Year Hours Accounting |
| `hper` | `integer` | `int` |  |  | Period Hours Accounting |
| `hemn` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `hsrn` | `integer` | `int` |  |  | Hours Accounting Sequence |
| `cpth` | `string` | `varchar` |  |  | Period Table for Hours Control. Max length: 6 |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rdas` | `timestamp` | `timestamp_ntz` |  |  | Rate Date (Sales) |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Fixed Currency Rate. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Fixed Currency Rate (keyword). Allowed values: , tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `orno` | `string` | `varchar` |  |  | Order Number. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Activity Line |
| `srnb` | `integer` | `int` |  |  | Line |
| `guid` | `string` | `varchar` |  |  | Unique ID. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Text |
| `cdf_sref__en_us` | `string` | `varchar` |  | `not_null` | (required) SAP Reference - base language. Max length: 30 |
| `cdf_susr__en_us` | `string` | `varchar` |  | `not_null` | (required) SAP User - base language. Max length: 30 |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `curc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `curs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cdoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin105 Invoice Text by Document |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cptf_fyea_fper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `hemn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
| `cpth_hyea_hper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
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

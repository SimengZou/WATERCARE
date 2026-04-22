# tffst120

LN tffst120 Statement Accounts table, Generated 2026-04-10T19:41:38Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffst120` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `fstm`, `accn` |
| **Column count** | 50 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `fstm` | `string` | `varchar` | `PK` | `not_null` | (required) Financial Statement. Max length: 8 |
| `accn` | `string` | `varchar` | `PK` | `not_null` | (required) Statement Account. Max length: 12 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Account Description - base language. Max length: 35 |
| `subl` | `integer` | `int` |  |  | Sublevel |
| `pacc` | `string` | `varchar` |  |  | Parent Statement Account. Max length: 12 |
| `dcsw` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `dcsw_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `rtyp` | `string` | `varchar` |  |  | Rate Type. Max length: 3 |
| `rhis` | `integer` | `int` |  |  | Use Historical Rates. Allowed values: 1, 2 |
| `rhis_kw` | `string` | `varchar` |  |  | Use Historical Rates (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `accs` | `integer` | `int` |  |  | Accounting Scheme. Allowed values: 1, 2, 3 |
| `accs_kw` | `string` | `varchar` |  |  | Accounting Scheme (keyword). Allowed values: tfgld.duac.pr.statutory, tfgld.duac.pr.complem, tfgld.duac.pr.both |
| `acct` | `integer` | `int` |  |  | Account Type. Allowed values: 10, 15, 20, 25, 30, 35, 99 |
| `acct_kw` | `string` | `varchar` |  |  | Account Type (keyword). Allowed values: tffst.acct.value, tffst.acct.text, tffst.acct.ratio, tffst.acct.rounding, tffst.acct.gain.loss, tffst.acct.balancing, tffst.acct.not.applicable |
| `cfsa` | `integer` | `int` |  |  | Cash Flow Statement Account. Allowed values: 1, 2 |
| `cfsa_kw` | `string` | `varchar` |  |  | Cash Flow Statement Account (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dbcr` | `integer` | `int` |  |  | Debit / Credit. Allowed values: 10, 20, 99 |
| `dbcr_kw` | `string` | `varchar` |  |  | Debit / Credit (keyword). Allowed values: tffst.dbcr.debit, tffst.dbcr.credit, tffst.dbcr.not.applicable |
| `dcgl` | `integer` | `int` |  |  | Default Gain/Loss Account. Allowed values: 1, 2 |
| `dcgl_kw` | `string` | `varchar` |  |  | Default Gain/Loss Account (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sign` | `integer` | `int` |  |  | Sign Switch. Allowed values: 1, 2, 3, 4, 5 |
| `sign_kw` | `string` | `varchar` |  |  | Sign Switch (keyword). Allowed values: tffst.sign.no.switch, tffst.sign.in.selection, tffst.sign.before.output, tffst.sign.select.bef.outp, tffst.sign.not.applicable |
| `acca` | `string` | `varchar` |  |  | Alternate Account. Max length: 12 |
| `muaa` | `integer` | `int` |  |  | Method of Alternate Account Usage. Allowed values: 1, 2, 3, 4 |
| `muaa_kw` | `string` | `varchar` |  |  | Method of Alternate Account Usage (keyword). Allowed values: tffst.muaa.total, tffst.muaa.individual, tffst.muaa.group, tffst.muaa.not.applicable |
| `acrd` | `string` | `varchar` |  |  | Rounding Difference Account. Max length: 12 |
| `cgla` | `string` | `varchar` |  |  | Currency Gain and Loss Account. Max length: 12 |
| `facc` | `string` | `varchar` |  |  | 100 Percent Account. Max length: 12 |
| `prta` | `integer` | `int` |  |  | Process Transactions for Account. Allowed values: 1, 2 |
| `prta_kw` | `string` | `varchar` |  |  | Process Transactions for Account (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pseq` | `integer` | `int` |  |  | Print Sequence |
| `atxt` | `integer` | `int` |  |  | Account Text |
| `fstm_acca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst120 Statement Accounts |
| `fstm_facc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst120 Statement Accounts |
| `fstm_acrd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst120 Statement Accounts |
| `fstm_pacc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst120 Statement Accounts |
| `fstm_cgla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst120 Statement Accounts |
| `fstm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst100 Financial Statements |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `atxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

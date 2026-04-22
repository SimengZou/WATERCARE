# tffst100

LN tffst100 Financial Statements table, Generated 2026-04-10T19:41:38Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffst100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `fstm` |
| **Column count** | 56 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `fstm` | `string` | `varchar` | `PK` | `not_null` | (required) Financial Statement. Max length: 8 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Statement Description - base language. Max length: 40 |
| `ouer` | `integer` | `int` |  |  | Only Used for External Report Definition. Allowed values: 1, 2 |
| `ouer_kw` | `string` | `varchar` |  |  | Only Used for External Report Definition (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `styp` | `integer` | `int` |  |  | Statement Type. Allowed values: 1, 2 |
| `styp_kw` | `string` | `varchar` |  |  | Statement Type (keyword). Allowed values: tffst.styp.fin.statement, tffst.styp.cons.statement |
| `usrc` | `string` | `varchar` |  |  | Created by User. Max length: 16 |
| `datc` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `usrm` | `string` | `varchar` |  |  | Last Modified by User. Max length: 16 |
| `datm` | `timestamp` | `timestamp_ntz` |  |  | Modification Date |
| `mdfs` | `timestamp` | `timestamp_ntz` |  |  | Modification Date for Selecting Values |
| `rcur` | `string` | `varchar` |  |  | Report Currency. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Default Rate Type. Max length: 3 |
| `cugl` | `integer` | `int` |  |  | Use Currency Gain/Loss Statement Account. Allowed values: 1, 2 |
| `cugl_kw` | `string` | `varchar` |  |  | Use Currency Gain/Loss Statement Account (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rhis` | `integer` | `int` |  |  | Historical Rates Allowed. Allowed values: 1, 2 |
| `rhis_kw` | `string` | `varchar` |  |  | Historical Rates Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `accs` | `integer` | `int` |  |  | Accounting Scheme. Allowed values: 1, 2, 3 |
| `accs_kw` | `string` | `varchar` |  |  | Accounting Scheme (keyword). Allowed values: tfgld.duac.pr.statutory, tfgld.duac.pr.complem, tfgld.duac.pr.both |
| `nstm` | `string` | `varchar` |  |  | Next Statement. Max length: 8 |
| `stlt` | `string` | `varchar` |  |  | Layout. Max length: 8 |
| `sltp` | `integer` | `int` |  |  | Layout Type. Allowed values: 10, 20, 30, 99 |
| `sltp_kw` | `string` | `varchar` |  |  | Layout Type (keyword). Allowed values: tffst.ltyp.statement, tffst.ltyp.annexure, tffst.ltyp.consolidated, tffst.ltyp.not.applicable |
| `pann` | `integer` | `int` |  |  | Use Annexure. Allowed values: 1, 2 |
| `pann_kw` | `string` | `varchar` |  |  | Use Annexure (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `salt` | `string` | `varchar` |  |  | Annexure Layout. Max length: 8 |
| `altp` | `integer` | `int` |  |  | Annexure Layout Type. Allowed values: 10, 20, 30, 99 |
| `altp_kw` | `string` | `varchar` |  |  | Annexure Layout Type (keyword). Allowed values: tffst.ltyp.statement, tffst.ltyp.annexure, tffst.ltyp.consolidated, tffst.ltyp.not.applicable |
| `stat` | `integer` | `int` |  |  | Statement Status. Allowed values: 1, 2, 99 |
| `stat_kw` | `string` | `varchar` |  |  | Statement Status (keyword). Allowed values: tffst.stat.registered, tffst.stat.approved, tffst.stat.not.applicable |
| `cagr` | `string` | `varchar` |  |  | Calculation Group. Max length: 3 |
| `ptra` | `integer` | `int` |  |  | Process Transactions. Allowed values: 1, 2, 3, 4 |
| `ptra_kw` | `string` | `varchar` |  |  | Process Transactions (keyword). Allowed values: tffst.ptra.none, tffst.ptra.ondemand, tffst.ptra.statement, tffst.ptra.account |
| `sthe` | `integer` | `int` |  |  | Statement Header |
| `anhe` | `integer` | `int` |  |  | Annexure Header |
| `taxo` | `string` | `varchar` |  |  | Last Imported Taxonomy. Max length: 9 |
| `vers` | `integer` | `int` |  |  | Last Imported Taxonomy Version |
| `txdt` | `timestamp` | `timestamp_ntz` |  |  | Date of Last Taxonomy Import |
| `rcur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `nstm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst100 Financial Statements |
| `sltp_stlt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst010 Statement Layouts |
| `altp_salt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst010 Statement Layouts |
| `cagr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst020 Calculation Groups |
| `sthe_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `anhe_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

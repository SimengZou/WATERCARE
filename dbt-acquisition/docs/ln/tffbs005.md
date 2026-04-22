# tffbs005

LN tffbs005 Budgets per Year table, Generated 2026-04-10T19:41:37Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffbs005` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `year`, `budg` |
| **Column count** | 40 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Fiscal Year |
| `budg` | `string` | `varchar` | `PK` | `not_null` | (required) Budget. Max length: 3 |
| `pbyr` | `integer` | `int` |  |  | Parent Budget Year |
| `pbud` | `string` | `varchar` |  |  | Parent Budget. Max length: 3 |
| `dbyr` | `integer` | `int` |  |  | Derived from Budget Year |
| `dbud` | `string` | `varchar` |  |  | Derived from Budget. Max length: 3 |
| `defi` | `integer` | `int` |  |  | Definitive Budget. Allowed values: 1, 2 |
| `defi_kw` | `string` | `varchar` |  |  | Definitive Budget (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `disb` | `string` | `varchar` |  |  | Distribution. Max length: 3 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Maintenance Date |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ratc_1` | `float` | `float` |  |  | Rate |
| `ratc_2` | `float` | `float` |  |  | Rate |
| `ratc_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `cbyr` | `integer` | `int` |  |  | Comparison Budget Year |
| `cbud` | `string` | `varchar` |  |  | Comparison Budget. Max length: 3 |
| `text` | `integer` | `int` |  |  | Text |
| `budg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs003 Budget |
| `pbyr_pbud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs005 Budgets per Year |
| `pbud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs003 Budget |
| `dbyr_dbud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs005 Budgets per Year |
| `dbud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs003 Budget |
| `disb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs001 Budget Distribution |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cbyr_cbud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs005 Budgets per Year |
| `cbud_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs003 Budget |
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

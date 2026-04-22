# tfacp001

LN tfacp001 Financial Supplier Groups table, Generated 2026-04-10T19:41:28Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `fisu` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `fisu` | `string` | `varchar` | `PK` | `not_null` | (required) Financial BP Group. Max length: 3 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `defp` | `string` | `varchar` |  |  | Default Purchase Type. Max length: 6 |
| `affi` | `integer` | `int` |  |  | Z5 Reporting. Allowed values: 1, 2, 3 |
| `affi_kw` | `string` | `varchar` |  |  | Z5 Reporting (keyword). Allowed values: tfacp.affi.affiliated, tfacp.affi.other, tfacp.affi.not.app |
| `text` | `integer` | `int` |  |  | Text |
| `defp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs201 Purchase Types |
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

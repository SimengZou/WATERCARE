# tffam808

LN tffam808 Periodic Book Value Summary table, Generated 2026-04-10T19:41:35Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam808` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `anbr`, `aext`, `book`, `year`, `prod` |
| **Column count** | 44 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `anbr` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Extension. Max length: 15 |
| `book` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Book. Max length: 10 |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Year |
| `prod` | `integer` | `int` | `PK` | `not_null` | (required) Period |
| `cost_1` | `float` | `float` |  |  | Current Cost |
| `cost_2` | `float` | `float` |  |  | Current Cost |
| `cost_3` | `float` | `float` |  |  | Current Cost |
| `ocst_1` | `float` | `float` |  |  | Original Cost |
| `ocst_2` | `float` | `float` |  |  | Original Cost |
| `ocst_3` | `float` | `float` |  |  | Original Cost |
| `ltdd_1` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_2` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_3` | `float` | `float` |  |  | Accumulated Depreciation |
| `s179_1` | `float` | `float` |  |  | Section 179 Value |
| `s179_2` | `float` | `float` |  |  | Section 179 Value |
| `s179_3` | `float` | `float` |  |  | Section 179 Value |
| `salv_1` | `float` | `float` |  |  | Salvage Value |
| `salv_2` | `float` | `float` |  |  | Salvage Value |
| `salv_3` | `float` | `float` |  |  | Salvage Value |
| `ytdd_1` | `float` | `float` |  |  | YTD Depreciation |
| `ytdd_2` | `float` | `float` |  |  | YTD Depreciation |
| `ytdd_3` | `float` | `float` |  |  | YTD Depreciation |
| `susp` | `integer` | `int` |  |  | Suspended. Allowed values: 1, 2 |
| `susp_kw` | `string` | `varchar` |  |  | Suspended (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcst_1` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_2` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_3` | `float` | `float` |  |  | Revaluation Cost |
| `ltdr_1` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_2` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_3` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ytdr_1` | `float` | `float` |  |  | YTD Revaluation Depreciation |
| `ytdr_2` | `float` | `float` |  |  | YTD Revaluation Depreciation |
| `ytdr_3` | `float` | `float` |  |  | YTD Revaluation Depreciation |
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

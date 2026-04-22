# tsclm330

LN tsclm330 Problems table, Generated 2026-04-10T19:42:29Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsclm330` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprl` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprl` | `string` | `varchar` | `PK` | `not_null` | (required) Problem. Max length: 10 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `prpr` | `integer` | `int` |  |  | Priority |
| `sear__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Argument - base language. Max length: 16 |
| `rpyn` | `integer` | `int` |  |  | Reported Problem. Allowed values: 1, 2 |
| `rpyn_kw` | `string` | `varchar` |  |  | Reported Problem (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `epyn` | `integer` | `int` |  |  | Expected Problem. Allowed values: 1, 2 |
| `epyn_kw` | `string` | `varchar` |  |  | Expected Problem (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `esyn` | `integer` | `int` |  |  | Actual Problem. Allowed values: 1, 2 |
| `esyn_kw` | `string` | `varchar` |  |  | Actual Problem (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `umtc` | `integer` | `int` |  |  | Use for Mean Time Calculations. Allowed values: 1, 2 |
| `umtc_kw` | `string` | `varchar` |  |  | Use for Mean Time Calculations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prpr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
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

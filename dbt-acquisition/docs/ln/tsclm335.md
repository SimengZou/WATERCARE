# tsclm335

LN tsclm335 Solutions table, Generated 2026-04-10T19:42:29Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsclm335` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cstn` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cstn` | `string` | `varchar` | `PK` | `not_null` | (required) Solution. Max length: 10 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `adur` | `float` | `float` |  |  | Duration |
| `exyn` | `integer` | `int` |  |  | Expected Solution. Allowed values: 1, 2 |
| `exyn_kw` | `string` | `varchar` |  |  | Expected Solution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `esyn` | `integer` | `int` |  |  | Actual Solution. Allowed values: 1, 2 |
| `esyn_kw` | `string` | `varchar` |  |  | Actual Solution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `umtc` | `integer` | `int` |  |  | Use for Mean Time Calculations. Allowed values: 1, 2 |
| `umtc_kw` | `string` | `varchar` |  |  | Use for Mean Time Calculations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
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

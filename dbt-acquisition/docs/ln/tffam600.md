# tffam600

LN tffam600 Books table, Generated 2026-04-10T19:41:33Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam600` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `book` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `book` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Book. Max length: 10 |
| `depr` | `integer` | `int` |  |  | Depreciable Book. Allowed values: 1, 2 |
| `depr_kw` | `string` | `varchar` |  |  | Depreciable Book (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `fdty` | `integer` | `int` |  |  | Federal Tax Type. Allowed values: 1, 2, 3, 4 |
| `fdty_kw` | `string` | `varchar` |  |  | Federal Tax Type (keyword). Allowed values: tffam.fdty.amt, tffam.fdty.ace, tffam.fdty.stan, tffam.fdty.na |
| `type` | `integer` | `int` |  |  | Book Type. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `type_kw` | `string` | `varchar` |  |  | Book Type (keyword). Allowed values: tffam.btyp.fina, tffam.btyp.fede, tffam.btyp.othe, tffam.btyp.comm, tffam.btyp.stat, tffam.btyp.calc, tffam.btyp.spec |
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

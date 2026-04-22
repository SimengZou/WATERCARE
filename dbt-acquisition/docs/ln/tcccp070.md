# tcccp070

LN tcccp070 Periods table, Generated 2026-04-10T19:40:59Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcccp070` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cpdt`, `yrno`, `peri` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cpdt` | `string` | `varchar` | `PK` | `not_null` | (required) Period Table Code. Max length: 6 |
| `yrno` | `integer` | `int` | `PK` | `not_null` | (required) Year |
| `peri` | `integer` | `int` | `PK` | `not_null` | (required) Period Number |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `cpdt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
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

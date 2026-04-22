# tcccp060

LN tcccp060 Period Tables table, Generated 2026-04-10T19:40:59Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcccp060` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cpdt` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cpdt` | `string` | `varchar` | `PK` | `not_null` | (required) Period Table Code. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `dura` | `integer` | `int` |  |  | Duration |
| `dtyp` | `integer` | `int` |  |  | Duration Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 100 |
| `dtyp_kw` | `string` | `varchar` |  |  | Duration Type (keyword). Allowed values: tcccp.kyrp.day, tcccp.kyrp.week, tcccp.kyrp.4weeks, tcccp.kyrp.month, tcccp.kyrp.quarter, tcccp.kyrp.halfyear, tcccp.kyrp.year, tcccp.kyrp.not.appl |
| `sttm` | `integer` | `int` |  |  | Start Time |
| `ychk` | `integer` | `int` |  |  | Check for Date in Year. Allowed values: 1, 2 |
| `ychk_kw` | `string` | `varchar` |  |  | Check for Date in Year (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mchk` | `integer` | `int` |  |  | Modification Allowed. Allowed values: 1, 2, 3 |
| `mchk_kw` | `string` | `varchar` |  |  | Modification Allowed (keyword). Allowed values: tcccp.mchk.yes, tcccp.mchk.restricted, tcccp.mchk.no |
| `gchk` | `integer` | `int` |  |  | Gaps Allowed between Years. Allowed values: 1, 2 |
| `gchk_kw` | `string` | `varchar` |  |  | Gaps Allowed between Years (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tzid` | `string` | `varchar` |  |  | Time Zone Code. Max length: 3 |
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

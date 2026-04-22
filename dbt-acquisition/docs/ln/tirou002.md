# tirou002

LN tirou002 Machine table, Generated 2026-04-10T19:41:49Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tirou002` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `mcno` |
| **Column count** | 29 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `mcno` | `string` | `varchar` | `PK` | `not_null` | (required) Machine. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `cwoc` | `string` | `varchar` |  |  | Work Center. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `mcrt_1` | `float` | `float` |  |  | Machine Rate |
| `mcrt_2` | `float` | `float` |  |  | Machine Rate |
| `mcrt_3` | `float` | `float` |  |  | Machine Rate |
| `cpcp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `mccp` | `float` | `float` |  |  | Week Capacity [Hours] |
| `mdcp` | `float` | `float` |  |  | Basic Day Capacity |
| `dque` | `float` | `float` |  |  | Desired Queue |
| `mere` | `integer` | `int` |  |  | MES Reporting. Allowed values: 1, 2 |
| `mere_kw` | `string` | `varchar` |  |  | MES Reporting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mewo` | `string` | `varchar` |  |  | MES Workplace. Max length: 256 |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirou001 Work Center |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cpcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
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

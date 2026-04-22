# amp

AMP Cube Data

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_amp` |
| **Materialization** | `incremental` |
| **Primary keys** | `ampversion`, `ampyear`, `ampperiod`, `ampproject`, `ampbusinessarea`, `ampcodes`, `ampemcodes`, `ampmeas` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ampversion` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `ampyear` | `string` | `varchar` | `PK` |  | Max length: 50 |
| `ampperiod` | `string` | `varchar` | `PK` |  | Max length: 50 |
| `ampproject` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `ampbusinessarea` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `ampcodes` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `ampemcodes` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `ampmeas` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `value` | `string` | `varchar` |  |  | Max length: 250 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  |  |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

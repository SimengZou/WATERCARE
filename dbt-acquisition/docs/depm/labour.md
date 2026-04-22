# labour

Labour Cube

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_labour` |
| **Materialization** | `incremental` |
| **Primary keys** | `version`, `year`, `period`, `analysis01`, `currency`, `analysis02`, `analysis03`, `analysis04`, `analysis05`, `account`, `position`, `labouremployeetype`, `labourtrxtype`, `labourlineno`, `labourmeas`, `measuretype` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `version` | `string` | `varchar` | `PK` |  | Max length: 50 |
| `year` | `string` | `varchar` | `PK` |  | Max length: 50 |
| `period` | `string` | `varchar` | `PK` |  | Max length: 50 |
| `currency` | `string` | `varchar` | `PK` |  | Max length: 50 |
| `analysis01` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `analysis02` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `analysis03` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `analysis04` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `analysis05` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `_account` | `string` | `varchar` |  |  | Max length: 100 |
| `position` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `labouremployeetype` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `labourtrxtype` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `labourlineno` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `labourmeas` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `measuretype` | `string` | `varchar` | `PK` |  | Max length: 100 |
| `value` | `string` | `varchar` |  |  | Max length: 300 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  |  |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

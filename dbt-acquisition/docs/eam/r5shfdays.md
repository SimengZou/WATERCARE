# r5shfdays

eam_R5SHFDAYS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5shfdays` |
| **Materialization** | `incremental` |
| **Primary keys** | `shd_shift`, `shd_day`, `shd_time` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `shd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SHD_LASTSAVED |
| `shd_day` | `float` | `float` | `PK` |  | SHD_DAY |
| `shd_hours` | `float` | `float` |  |  | SHD_HOURS |
| `shd_updatecount` | `float` | `float` |  |  | SHD_UPDATECOUNT |
| `shd_shift` | `string` | `varchar` | `PK` |  | SHD_SHIFT |
| `shd_time` | `float` | `float` | `PK` |  | SHD_TIME |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

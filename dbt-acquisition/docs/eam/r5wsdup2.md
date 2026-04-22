# r5wsdup2

eam_R5WSDUP2

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wsdup2` |
| **Materialization** | `incremental` |
| **Primary keys** | `wd2_code` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wd2_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WD2_LASTSAVED |
| `wd2_desc` | `string` | `varchar` |  |  | WD2_DESC |
| `wd2_type` | `string` | `varchar` |  |  | WD2_TYPE |
| `wd2_updatecount` | `float` | `float` |  |  | WD2_UPDATECOUNT |
| `wd2_code` | `string` | `varchar` | `PK` | `unique` | WD2_CODE |
| `wd2_time` | `timestamp` | `timestamp_ntz` |  |  | WD2_TIME |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

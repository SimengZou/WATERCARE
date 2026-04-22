# r5jobs

eam_R5JOBS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5jobs` |
| **Materialization** | `incremental` |
| **Primary keys** | `job_name` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `job_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | JOB_LASTSAVED |
| `job_description` | `string` | `varchar` |  |  | JOB_DESCRIPTION |
| `job_class` | `string` | `varchar` |  |  | JOB_CLASS |
| `job_updatecount` | `float` | `float` |  |  | JOB_UPDATECOUNT |
| `job_partnerid` | `string` | `varchar` |  |  | JOB_PARTNERID |
| `job_name` | `string` | `varchar` | `PK` | `unique` | JOB_NAME |
| `job_type` | `string` | `varchar` |  |  | JOB_TYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

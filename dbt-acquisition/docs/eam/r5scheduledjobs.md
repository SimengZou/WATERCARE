# r5scheduledjobs

eam_R5SCHEDULEDJOBS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5scheduledjobs` |
| **Materialization** | `incremental` |
| **Primary keys** | `scj_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `scj_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SCJ_LASTSAVED |
| `scj_jobname` | `string` | `varchar` |  |  | SCJ_JOBNAME |
| `scj_schcode` | `string` | `varchar` |  |  | SCJ_SCHCODE |
| `scj_active` | `string` | `varchar` |  |  | SCJ_ACTIVE |
| `scj_broken` | `string` | `varchar` |  |  | SCJ_BROKEN |
| `scj_nextrun` | `timestamp` | `timestamp_ntz` |  |  | SCJ_NEXTRUN |
| `scj_updatecount` | `float` | `float` |  |  | SCJ_UPDATECOUNT |
| `scj_serverid` | `string` | `varchar` |  |  | SCJ_SERVERID |
| `scj_type` | `string` | `varchar` |  |  | SCJ_TYPE |
| `scj_code` | `string` | `varchar` | `PK` | `unique` | SCJ_CODE |
| `scj_lastrun` | `timestamp` | `timestamp_ntz` |  |  | SCJ_LASTRUN |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

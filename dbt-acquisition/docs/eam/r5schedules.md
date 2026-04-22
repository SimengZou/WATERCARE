# r5schedules

eam_R5SCHEDULES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5schedules` |
| **Materialization** | `incremental` |
| **Primary keys** | `sch_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `sch_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SCH_LASTSAVED |
| `sch_name` | `string` | `varchar` |  |  | SCH_NAME |
| `sch_description` | `string` | `varchar` |  |  | SCH_DESCRIPTION |
| `sch_type` | `string` | `varchar` |  |  | SCH_TYPE |
| `sch_month` | `string` | `varchar` |  |  | SCH_MONTH |
| `sch_dayofweek` | `string` | `varchar` |  |  | SCH_DAYOFWEEK |
| `sch_hour` | `string` | `varchar` |  |  | SCH_HOUR |
| `sch_minute` | `string` | `varchar` |  |  | SCH_MINUTE |
| `sch_updatecount` | `float` | `float` |  |  | SCH_UPDATECOUNT |
| `sch_code` | `string` | `varchar` | `PK` | `unique` | SCH_CODE |
| `sch_dayofmonth` | `string` | `varchar` |  |  | SCH_DAYOFMONTH |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

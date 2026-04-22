# r5usage

eam_R5USAGE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5usage` |
| **Materialization** | `incremental` |
| **Primary keys** | `sus_id` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `sus_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SUS_LASTSAVED |
| `sus_target_tab` | `string` | `varchar` |  |  | SUS_TARGET_TAB |
| `sus_serverid` | `string` | `varchar` |  |  | SUS_SERVERID |
| `sus_sessionid` | `float` | `float` |  |  | SUS_SESSIONID |
| `sus_useragent` | `string` | `varchar` |  |  | SUS_USERAGENT |
| `sus_processing_time` | `float` | `float` |  |  | SUS_PROCESSING_TIME |
| `sus_action_date` | `timestamp` | `timestamp_ntz` |  |  | SUS_ACTION_DATE |
| `sus_updatecount` | `float` | `float` |  |  | SUS_UPDATECOUNT |
| `sus_id` | `string` | `varchar` | `PK` | `unique` | SUS_ID |
| `sus_action` | `string` | `varchar` |  |  | SUS_ACTION |
| `sus_type` | `string` | `varchar` |  |  | SUS_TYPE |
| `sus_subtype` | `string` | `varchar` |  |  | SUS_SUBTYPE |
| `sus_target` | `string` | `varchar` |  |  | SUS_TARGET |
| `sus_target_parent` | `string` | `varchar` |  |  | SUS_TARGET_PARENT |
| `sus_userid` | `string` | `varchar` |  |  | SUS_USERID |
| `sus_tenant` | `string` | `varchar` |  |  | SUS_TENANT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

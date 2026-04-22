# r5alertwo

eam_R5ALERTWO

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alertwo` |
| **Materialization** | `incremental` |
| **Primary keys** | `alw_alert` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `alw_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ALW_LASTSAVED |
| `alw_delayuom` | `string` | `varchar` |  |  | ALW_DELAYUOM |
| `alw_standwork` | `string` | `varchar` |  |  | ALW_STANDWORK |
| `alw_workorderorg` | `string` | `varchar` |  |  | ALW_WORKORDERORG |
| `alw_objectfieldid` | `float` | `float` |  |  | ALW_OBJECTFIELDID |
| `alw_objectorgfieldid` | `float` | `float` |  |  | ALW_OBJECTORGFIELDID |
| `alw_workorderorgfieldid` | `float` | `float` |  |  | ALW_WORKORDERORGFIELDID |
| `alw_problemcodefieldid` | `float` | `float` |  |  | ALW_PROBLEMCODEFIELDID |
| `alw_jobtypefieldid` | `float` | `float` |  |  | ALW_JOBTYPEFIELDID |
| `alw_priorityfieldid` | `float` | `float` |  |  | ALW_PRIORITYFIELDID |
| `alw_durationfieldid` | `float` | `float` |  |  | ALW_DURATIONFIELDID |
| `alw_schedstartfieldid` | `float` | `float` |  |  | ALW_SCHEDSTARTFIELDID |
| `alw_requeststartfieldid` | `float` | `float` |  |  | ALW_REQUESTSTARTFIELDID |
| `alw_requestendfieldid` | `float` | `float` |  |  | ALW_REQUESTENDFIELDID |
| `alw_wodesc` | `string` | `varchar` |  |  | ALW_WODESC |
| `alw_wocomment` | `string` | `varchar` |  |  | ALW_WOCOMMENT. Max length: 0 |
| `alw_updatecount` | `float` | `float` |  |  | ALW_UPDATECOUNT |
| `alw_includenonconformities` | `string` | `varchar` |  |  | ALW_INCLUDENONCONFORMITIES |
| `alw_duenonconformitiesonly` | `string` | `varchar` |  |  | ALW_DUENONCONFORMITIESONLY |
| `alw_alert` | `string` | `varchar` | `PK` | `unique` | ALW_ALERT |
| `alw_delay` | `float` | `float` |  |  | ALW_DELAY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

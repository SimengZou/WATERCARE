# r5actschedulesview

eam_R5ACTSCHEDULESVIEW

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5actschedulesview` |
| **Materialization** | `incremental` |
| **Column count** | 25 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `acs_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ACS_LASTSAVED |
| `acs_comment` | `string` | `varchar` |  |  | ACS_COMMENT |
| `acs_mrc` | `string` | `varchar` |  |  | ACS_MRC |
| `acs_trade` | `string` | `varchar` |  |  | ACS_TRADE |
| `acs_object` | `string` | `varchar` |  |  | ACS_OBJECT |
| `acs_sched` | `timestamp` | `timestamp_ntz` |  |  | ACS_SCHED |
| `acs_starttime` | `string` | `varchar` |  |  | ACS_STARTTIME |
| `acs_endtime` | `string` | `varchar` |  |  | ACS_ENDTIME |
| `acs_hours` | `float` | `float` |  |  | ACS_HOURS |
| `acs_responsible` | `string` | `varchar` |  |  | ACS_RESPONSIBLE |
| `acs_shift` | `string` | `varchar` |  |  | ACS_SHIFT |
| `acs_scheduler` | `string` | `varchar` |  |  | ACS_SCHEDULER |
| `acs_frozen` | `string` | `varchar` |  |  | ACS_FROZEN |
| `acs_object_org` | `string` | `varchar` |  |  | ACS_OBJECT_ORG |
| `acs_mpproj` | `string` | `varchar` |  |  | ACS_MPPROJ |
| `acs_updatecount` | `float` | `float` |  |  | ACS_UPDATECOUNT |
| `acs_code` | `string` | `varchar` |  |  | ACS_CODE |
| `acs_event` | `string` | `varchar` |  |  | ACS_EVENT |
| `acs_activity` | `float` | `float` |  |  | ACS_ACTIVITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5pmplansession

eam_R5PMPLANSESSION

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pmplansession` |
| **Materialization** | `incremental` |
| **Primary keys** | `pps_pk` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pps_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PPS_LASTSAVED |
| `pps_sessionid` | `float` | `float` |  |  | PPS_SESSIONID |
| `pps_ppopk` | `float` | `float` |  |  | PPS_PPOPK |
| `pps_pmrevision` | `float` | `float` |  |  | PPS_PMREVISION |
| `pps_dueweekofmonth` | `string` | `varchar` |  |  | PPS_DUEWEEKOFMONTH |
| `pps_duedayofweek` | `float` | `float` |  |  | PPS_DUEDAYOFWEEK |
| `pps_duedate` | `timestamp` | `timestamp_ntz` |  |  | PPS_DUEDATE |
| `pps_nestingref` | `string` | `varchar` |  |  | PPS_NESTINGREF |
| `pps_ignorefreqwarning` | `string` | `varchar` |  |  | PPS_IGNOREFREQWARNING |
| `pps_ignorerangewarning` | `string` | `varchar` |  |  | PPS_IGNORERANGEWARNING |
| `pps_updatecount` | `float` | `float` |  |  | PPS_UPDATECOUNT |
| `pps_pk` | `float` | `float` | `PK` | `unique` | PPS_PK |
| `pps_locked` | `string` | `varchar` |  |  | PPS_LOCKED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5objectsurvey

eam_R5OBJECTSURVEY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5objectsurvey` |
| **Materialization** | `incremental` |
| **Primary keys** | `obs_object`, `obs_object_org`, `obs_type`, `obs_levelpk` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `obs_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OBS_LASTSAVED |
| `obs_object_org` | `string` | `varchar` | `PK` |  | OBS_OBJECT_ORG |
| `obs_type` | `string` | `varchar` | `PK` |  | OBS_TYPE |
| `obs_levelpk` | `string` | `varchar` | `PK` |  | OBS_LEVELPK |
| `obs_answerpk` | `string` | `varchar` |  |  | OBS_ANSWERPK |
| `obs_updatecount` | `float` | `float` |  |  | OBS_UPDATECOUNT |
| `obs_dateeffective` | `timestamp` | `timestamp_ntz` |  |  | OBS_DATEEFFECTIVE |
| `obs_calculatedanswer` | `string` | `varchar` |  |  | OBS_CALCULATEDANSWER |
| `obs_calculatedvalue` | `float` | `float` |  |  | OBS_CALCULATEDVALUE |
| `obs_datelastcalculated` | `timestamp` | `timestamp_ntz` |  |  | OBS_DATELASTCALCULATED |
| `obs_workorder` | `string` | `varchar` |  |  | OBS_WORKORDER |
| `obs_operatorchecklist` | `string` | `varchar` |  |  | OBS_OPERATORCHECKLIST |
| `obs_object` | `string` | `varchar` | `PK` |  | OBS_OBJECT |
| `obs_value` | `float` | `float` |  |  | OBS_VALUE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

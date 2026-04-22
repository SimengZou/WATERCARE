# r5jobtypeauth

eam_R5JOBTYPEAUTH

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5jobtypeauth` |
| **Materialization** | `incremental` |
| **Primary keys** | `jta_group`, `jta_jobtype`, `jta_status` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `jta_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | JTA_LASTSAVED |
| `jta_jobtype` | `string` | `varchar` | `PK` |  | JTA_JOBTYPE |
| `jta_insert` | `string` | `varchar` |  |  | JTA_INSERT |
| `jta_update` | `string` | `varchar` |  |  | JTA_UPDATE |
| `jta_updatecount` | `float` | `float` |  |  | JTA_UPDATECOUNT |
| `jta_updated` | `timestamp` | `timestamp_ntz` |  |  | JTA_UPDATED |
| `jta_status` | `string` | `varchar` | `PK` |  | JTA_STATUS |
| `jta_group` | `string` | `varchar` | `PK` |  | JTA_GROUP |
| `jta_delete` | `string` | `varchar` |  |  | JTA_DELETE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

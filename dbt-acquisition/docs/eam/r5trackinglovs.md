# r5trackinglovs

eam_R5TRACKINGLOVS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5trackinglovs` |
| **Materialization** | `incremental` |
| **Primary keys** | `tkv_code` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tkv_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TKV_LASTSAVED |
| `tkv_sql` | `string` | `varchar` |  |  | TKV_SQL |
| `tkv_updatecount` | `float` | `float` |  |  | TKV_UPDATECOUNT |
| `tkv_code` | `string` | `varchar` | `PK` | `unique` | TKV_CODE |
| `tkv_updated` | `timestamp` | `timestamp_ntz` |  |  | TKV_UPDATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

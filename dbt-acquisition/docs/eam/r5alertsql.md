# r5alertsql

eam_R5ALERTSQL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alertsql` |
| **Materialization** | `incremental` |
| **Primary keys** | `als_alert`, `als_rtype` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `als_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ALS_LASTSAVED |
| `als_rtype` | `string` | `varchar` | `PK` |  | ALS_RTYPE |
| `als_stmt` | `string` | `varchar` |  |  | ALS_STMT |
| `als_abortonfailure` | `string` | `varchar` |  |  | ALS_ABORTONFAILURE |
| `als_active` | `string` | `varchar` |  |  | ALS_ACTIVE |
| `als_comment` | `string` | `varchar` |  |  | ALS_COMMENT |
| `als_updatecount` | `float` | `float` |  |  | ALS_UPDATECOUNT |
| `als_alert` | `string` | `varchar` | `PK` |  | ALS_ALERT |
| `als_includepreview` | `string` | `varchar` |  |  | ALS_INCLUDEPREVIEW |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

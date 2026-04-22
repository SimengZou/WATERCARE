# r5userviews

eam_R5USERVIEWS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userviews` |
| **Materialization** | `incremental` |
| **Primary keys** | `uvw_code` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `uvw_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UVW_LASTSAVED |
| `uvw_desc` | `string` | `varchar` |  |  | UVW_DESC |
| `uvw_name` | `string` | `varchar` |  |  | UVW_NAME |
| `uvw_stmt` | `string` | `varchar` |  |  | UVW_STMT |
| `uvw_active` | `string` | `varchar` |  |  | UVW_ACTIVE |
| `uvw_updatecount` | `float` | `float` |  |  | UVW_UPDATECOUNT |
| `uvw_code` | `string` | `varchar` | `PK` | `unique` | UVW_CODE |
| `uvw_note` | `string` | `varchar` |  |  | UVW_NOTE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

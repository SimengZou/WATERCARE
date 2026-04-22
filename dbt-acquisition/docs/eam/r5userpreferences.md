# r5userpreferences

eam_R5USERPREFERENCES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userpreferences` |
| **Materialization** | `incremental` |
| **Primary keys** | `usp_user`, `usp_type`, `usp_code` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `usp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | USP_LASTSAVED |
| `usp_type` | `string` | `varchar` | `PK` |  | USP_TYPE |
| `usp_value` | `string` | `varchar` |  |  | USP_VALUE |
| `usp_updatecount` | `float` | `float` |  |  | USP_UPDATECOUNT |
| `usp_user` | `string` | `varchar` | `PK` |  | USP_USER |
| `usp_code` | `string` | `varchar` | `PK` |  | USP_CODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

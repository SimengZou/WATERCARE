# r5errsource

eam_R5ERRSOURCE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5errsource` |
| **Materialization** | `incremental` |
| **Primary keys** | `ers_source`, `ers_type`, `ers_number` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ers_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ERS_LASTSAVED |
| `ers_type` | `string` | `varchar` | `PK` |  | ERS_TYPE |
| `ers_desc` | `string` | `varchar` |  |  | ERS_DESC |
| `ers_code` | `string` | `varchar` |  |  | ERS_CODE |
| `ers_name` | `string` | `varchar` |  |  | ERS_NAME |
| `ers_updatecount` | `float` | `float` |  |  | ERS_UPDATECOUNT |
| `ers_source` | `string` | `varchar` | `PK` |  | ERS_SOURCE |
| `ers_number` | `float` | `float` | `PK` |  | ERS_NUMBER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

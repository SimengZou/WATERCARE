# r5screencache

eam_R5SCREENCACHE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5screencache` |
| **Materialization** | `incremental` |
| **Primary keys** | `snc_user`, `snc_function` |
| **Column count** | 10 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `snc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SNC_LASTSAVED |
| `snc_updatecount` | `float` | `float` |  |  | SNC_UPDATECOUNT |
| `snc_user` | `string` | `varchar` | `PK` |  | SNC_USER |
| `snc_function` | `string` | `varchar` | `PK` |  | SNC_FUNCTION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

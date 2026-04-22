# r5wsdup1

eam_R5WSDUP1

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wsdup1` |
| **Materialization** | `incremental` |
| **Primary keys** | `wd1_code` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wd1_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WD1_LASTSAVED |
| `wd1_time` | `timestamp` | `timestamp_ntz` |  |  | WD1_TIME |
| `wd1_code` | `string` | `varchar` | `PK` | `unique` | WD1_CODE |
| `wd1_updatecount` | `float` | `float` |  |  | WD1_UPDATECOUNT |
| `wd1_desc` | `string` | `varchar` |  |  | WD1_DESC |
| `wd1_type` | `string` | `varchar` |  |  | WD1_TYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

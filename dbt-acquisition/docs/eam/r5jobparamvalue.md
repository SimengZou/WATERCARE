# r5jobparamvalue

eam_R5JOBPARAMVALUE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5jobparamvalue` |
| **Materialization** | `incremental` |
| **Primary keys** | `jpv_code`, `jpv_line` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `jpv_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | JPV_LASTSAVED |
| `jpv_line` | `float` | `float` | `PK` |  | JPV_LINE |
| `jpv_value` | `string` | `varchar` |  |  | JPV_VALUE |
| `jpv_updatecount` | `float` | `float` |  |  | JPV_UPDATECOUNT |
| `jpv_code` | `string` | `varchar` | `PK` |  | JPV_CODE |
| `jpv_key` | `string` | `varchar` |  |  | JPV_KEY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5reportgroupby

eam_R5REPORTGROUPBY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reportgroupby` |
| **Materialization** | `incremental` |
| **Primary keys** | `rgp_function`, `rgp_line` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rgp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RGP_LASTSAVED |
| `rgp_line` | `float` | `float` | `PK` |  | RGP_LINE |
| `rgp_groupfields` | `string` | `varchar` |  |  | RGP_GROUPFIELDS |
| `rgp_updatecount` | `float` | `float` |  |  | RGP_UPDATECOUNT |
| `rgp_updated` | `timestamp` | `timestamp_ntz` |  |  | RGP_UPDATED |
| `rgp_function` | `string` | `varchar` | `PK` |  | RGP_FUNCTION |
| `rgp_default` | `string` | `varchar` |  |  | RGP_DEFAULT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

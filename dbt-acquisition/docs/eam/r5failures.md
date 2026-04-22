# r5failures

eam_R5FAILURES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5failures` |
| **Materialization** | `incremental` |
| **Primary keys** | `fal_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `fal_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FAL_LASTSAVED |
| `fal_desc` | `string` | `varchar` |  |  | FAL_DESC |
| `fal_gen` | `string` | `varchar` |  |  | FAL_GEN |
| `fal_updatecount` | `float` | `float` |  |  | FAL_UPDATECOUNT |
| `fal_created` | `timestamp` | `timestamp_ntz` |  |  | FAL_CREATED |
| `fal_partfailure` | `string` | `varchar` |  |  | FAL_PARTFAILURE |
| `fal_notused` | `string` | `varchar` |  |  | FAL_NOTUSED |
| `fal_enableworkorders` | `string` | `varchar` |  |  | FAL_ENABLEWORKORDERS |
| `fal_group` | `string` | `varchar` |  |  | FAL_GROUP |
| `fal_code` | `string` | `varchar` | `PK` | `unique` | FAL_CODE |
| `fal_updated` | `timestamp` | `timestamp_ntz` |  |  | FAL_UPDATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

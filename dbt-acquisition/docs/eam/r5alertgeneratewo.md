# r5alertgeneratewo

eam_R5ALERTGENERATEWO

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alertgeneratewo` |
| **Materialization** | `incremental` |
| **Primary keys** | `agw_alert` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `agw_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AGW_LASTSAVED |
| `agw_type` | `string` | `varchar` |  |  | AGW_TYPE |
| `agw_updatecount` | `float` | `float` |  |  | AGW_UPDATECOUNT |
| `agw_alert` | `string` | `varchar` | `PK` | `unique` | AGW_ALERT |
| `agw_generatethroughfieldid` | `float` | `float` |  |  | AGW_GENERATETHROUGHFIELDID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

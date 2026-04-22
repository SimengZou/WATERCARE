# r5wsevents

eam_R5WSEVENTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wsevents` |
| **Materialization** | `incremental` |
| **Primary keys** | `wse_code` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wse_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WSE_LASTSAVED |
| `wse_desc` | `string` | `varchar` |  |  | WSE_DESC |
| `wse_base_event` | `string` | `varchar` |  |  | WSE_BASE_EVENT |
| `wse_msg_template` | `string` | `varchar` |  |  | WSE_MSG_TEMPLATE. Max length: 0 |
| `wse_updatecount` | `float` | `float` |  |  | WSE_UPDATECOUNT |
| `wse_mekey` | `string` | `varchar` |  |  | WSE_MEKEY |
| `wse_code` | `string` | `varchar` | `PK` | `unique` | WSE_CODE |
| `wse_filter_processor` | `string` | `varchar` |  |  | WSE_FILTER_PROCESSOR |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

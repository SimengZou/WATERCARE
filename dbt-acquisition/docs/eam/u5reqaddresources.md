# u5reqaddresources

eam_U5REQADDRESOURCES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5reqaddresources` |
| **Materialization** | `incremental` |
| **Primary keys** | `rar_transid` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `rar_event` | `string` | `varchar` |  |  | RAR_EVENT |
| `rar_type` | `string` | `varchar` |  |  | RAR_TYPE |
| `rar_notes` | `string` | `varchar` |  |  | RAR_NOTES |
| `rar_hazards` | `string` | `varchar` |  |  | RAR_HAZARDS |
| `rar_resultwonum` | `string` | `varchar` |  |  | RAR_RESULTWONUM |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `rar_transid` | `string` | `varchar` | `PK` | `unique` | RAR_TRANSID |
| `rar_raisedby` | `string` | `varchar` |  |  | RAR_RAISEDBY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

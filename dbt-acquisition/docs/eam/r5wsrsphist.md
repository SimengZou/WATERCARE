# r5wsrsphist

eam_R5WSRSPHIST

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wsrsphist` |
| **Materialization** | `incremental` |
| **Primary keys** | `wsr_message`, `wsr_process` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wsr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WSR_LASTSAVED |
| `wsr_address` | `string` | `varchar` |  |  | WSR_ADDRESS |
| `wsr_message` | `string` | `varchar` | `PK` |  | WSR_MESSAGE |
| `wsr_dataarea` | `float` | `float` |  |  | WSR_DATAAREA |
| `wsr_time` | `timestamp` | `timestamp_ntz` |  |  | WSR_TIME |
| `wsr_status` | `string` | `varchar` |  |  | WSR_STATUS |
| `wsr_rstatus` | `string` | `varchar` |  |  | WSR_RSTATUS |
| `wsr_status_message` | `string` | `varchar` |  |  | WSR_STATUS_MESSAGE |
| `wsr_updatecount` | `float` | `float` |  |  | WSR_UPDATECOUNT |
| `wsr_process` | `string` | `varchar` | `PK` |  | WSR_PROCESS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

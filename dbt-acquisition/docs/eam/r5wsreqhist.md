# r5wsreqhist

eam_R5WSREQHIST

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wsreqhist` |
| **Materialization** | `incremental` |
| **Primary keys** | `wsq_message`, `wsq_process` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wsq_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WSQ_LASTSAVED |
| `wsq_process` | `string` | `varchar` | `PK` |  | WSQ_PROCESS |
| `wsq_time` | `timestamp` | `timestamp_ntz` |  |  | WSQ_TIME |
| `wsq_status` | `string` | `varchar` |  |  | WSQ_STATUS |
| `wsq_status_message` | `string` | `varchar` |  |  | WSQ_STATUS_MESSAGE |
| `wsq_dataarea` | `float` | `float` |  |  | WSQ_DATAAREA |
| `wsq_updatecount` | `float` | `float` |  |  | WSQ_UPDATECOUNT |
| `wsq_message` | `string` | `varchar` | `PK` |  | WSQ_MESSAGE |
| `wsq_rstatus` | `string` | `varchar` |  |  | WSQ_RSTATUS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5mailtemplate

eam_R5MAILTEMPLATE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mailtemplate` |
| **Materialization** | `incremental` |
| **Primary keys** | `mat_code` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mat_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MAT_LASTSAVED |
| `mat_subject` | `string` | `varchar` |  |  | MAT_SUBJECT |
| `mat_text` | `string` | `varchar` |  |  | MAT_TEXT |
| `mat_mail` | `string` | `varchar` |  |  | MAT_MAIL |
| `mat_updatecount` | `float` | `float` |  |  | MAT_UPDATECOUNT |
| `mat_code` | `string` | `varchar` | `PK` | `unique` | MAT_CODE |
| `mat_fromemail` | `string` | `varchar` |  |  | MAT_FROMEMAIL |
| `mat_pushnotification` | `string` | `varchar` |  |  | MAT_PUSHNOTIFICATION |
| `mat_email` | `string` | `varchar` |  |  | MAT_EMAIL |
| `mat_notebookemails` | `string` | `varchar` |  |  | MAT_NOTEBOOKEMAILS |
| `mat_desc` | `string` | `varchar` |  |  | MAT_DESC |
| `mat_report` | `string` | `varchar` |  |  | MAT_REPORT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

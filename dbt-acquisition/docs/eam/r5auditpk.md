# r5auditpk

eam_R5AUDITPK

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5auditpk` |
| **Materialization** | `incremental` |
| **Primary keys** | `apk_table`, `apk_seqno` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `apk_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | APK_LASTSAVED |
| `apk_seqno` | `float` | `float` | `PK` |  | APK_SEQNO |
| `apk_column` | `string` | `varchar` |  |  | APK_COLUMN |
| `apk_datatype` | `string` | `varchar` |  |  | APK_DATATYPE |
| `apk_updated` | `timestamp` | `timestamp_ntz` |  |  | APK_UPDATED |
| `apk_table` | `string` | `varchar` | `PK` |  | APK_TABLE |
| `apk_updatecount` | `float` | `float` |  |  | APK_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5opvarattributes

eam_R5OPVARATTRIBUTES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5opvarattributes` |
| **Materialization** | `incremental` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `oat_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OAT_LASTSAVED |
| `oat_desc` | `string` | `varchar` |  |  | OAT_DESC |
| `oat_type` | `float` | `float` |  |  | OAT_TYPE |
| `oat_audituser` | `string` | `varchar` |  |  | OAT_AUDITUSER |
| `oat_audittimestamp` | `timestamp` | `timestamp_ntz` |  |  | OAT_AUDITTIMESTAMP |
| `oat_updatecount` | `float` | `float` |  |  | OAT_UPDATECOUNT |
| `oat_id` | `float` | `float` |  |  | OAT_ID |
| `oat_label` | `string` | `varchar` |  |  | OAT_LABEL |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5customtexts

eam_R5CUSTOMTEXTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5customtexts` |
| **Materialization** | `incremental` |
| **Primary keys** | `ctt_pool`, `ctt_lang`, `ctt_length` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ctt_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CTT_LASTSAVED |
| `ctt_lang` | `string` | `varchar` | `PK` |  | CTT_LANG |
| `ctt_length` | `float` | `float` | `PK` |  | CTT_LENGTH |
| `ctt_origtext` | `string` | `varchar` |  |  | CTT_ORIGTEXT |
| `ctt_date` | `timestamp` | `timestamp_ntz` |  |  | CTT_DATE |
| `ctt_updatecount` | `float` | `float` |  |  | CTT_UPDATECOUNT |
| `ctt_pool` | `string` | `varchar` | `PK` |  | CTT_POOL |
| `ctt_text` | `string` | `varchar` |  |  | CTT_TEXT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

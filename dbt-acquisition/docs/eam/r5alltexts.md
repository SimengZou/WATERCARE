# r5alltexts

eam_R5ALLTEXTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alltexts` |
| **Materialization** | `incremental` |
| **Primary keys** | `atx_code`, `atx_texttype`, `atx_lang` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `atx_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ATX_LASTSAVED |
| `atx_texttype` | `string` | `varchar` | `PK` |  | ATX_TEXTTYPE |
| `atx_lang` | `string` | `varchar` | `PK` |  | ATX_LANG |
| `atx_lastmodified` | `timestamp` | `timestamp_ntz` |  |  | ATX_LASTMODIFIED |
| `atx_updatecount` | `float` | `float` |  |  | ATX_UPDATECOUNT |
| `atx_code` | `string` | `varchar` | `PK` |  | ATX_CODE |
| `atx_text` | `string` | `varchar` |  |  | ATX_TEXT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

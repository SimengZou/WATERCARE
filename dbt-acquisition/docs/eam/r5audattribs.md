# r5audattribs

eam_R5AUDATTRIBS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5audattribs` |
| **Materialization** | `incremental` |
| **Primary keys** | `aat_code` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `aat_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AAT_LASTSAVED |
| `aat_table` | `string` | `varchar` |  |  | AAT_TABLE |
| `aat_column` | `string` | `varchar` |  |  | AAT_COLUMN |
| `aat_text` | `string` | `varchar` |  |  | AAT_TEXT |
| `aat_enteredby` | `string` | `varchar` |  |  | AAT_ENTEREDBY |
| `aat_insert` | `string` | `varchar` |  |  | AAT_INSERT |
| `aat_update` | `string` | `varchar` |  |  | AAT_UPDATE |
| `aat_delete` | `string` | `varchar` |  |  | AAT_DELETE |
| `aat_updatecount` | `float` | `float` |  |  | AAT_UPDATECOUNT |
| `aat_updated` | `timestamp` | `timestamp_ntz` |  |  | AAT_UPDATED |
| `aat_code` | `float` | `float` | `PK` | `unique` | AAT_CODE |
| `aat_comment` | `string` | `varchar` |  |  | AAT_COMMENT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

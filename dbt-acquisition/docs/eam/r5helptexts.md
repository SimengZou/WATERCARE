# r5helptexts

eam_R5HELPTEXTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5helptexts` |
| **Materialization** | `incremental` |
| **Primary keys** | `hel_function`, `hel_item`, `hel_lang` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `hel_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | HEL_LASTSAVED |
| `hel_item` | `string` | `varchar` | `PK` |  | HEL_ITEM |
| `hel_text` | `string` | `varchar` |  |  | HEL_TEXT |
| `hel_lang` | `string` | `varchar` | `PK` |  | HEL_LANG |
| `hel_drilldown` | `string` | `varchar` |  |  | HEL_DRILLDOWN |
| `hel_changed` | `string` | `varchar` |  |  | HEL_CHANGED |
| `hel_updatecount` | `float` | `float` |  |  | HEL_UPDATECOUNT |
| `hel_function` | `string` | `varchar` | `PK` |  | HEL_FUNCTION |
| `hel_pool` | `string` | `varchar` |  |  | HEL_POOL |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

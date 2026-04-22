# r5boilertexts

eam_R5BOILERTEXTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5boilertexts` |
| **Materialization** | `incremental` |
| **Primary keys** | `bot_function`, `bot_number`, `bot_lang` |
| **Column count** | 28 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `bot_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | BOT_LASTSAVED |
| `bot_length` | `float` | `float` |  |  | BOT_LENGTH |
| `bot_xpos` | `float` | `float` |  |  | BOT_XPOS |
| `bot_ypos` | `float` | `float` |  |  | BOT_YPOS |
| `bot_lang` | `string` | `varchar` | `PK` |  | BOT_LANG |
| `bot_text` | `string` | `varchar` |  |  | BOT_TEXT |
| `bot_dest` | `string` | `varchar` |  |  | BOT_DEST |
| `bot_page` | `string` | `varchar` |  |  | BOT_PAGE |
| `bot_fld1` | `string` | `varchar` |  |  | BOT_FLD1 |
| `bot_fld2` | `string` | `varchar` |  |  | BOT_FLD2 |
| `bot_prtp` | `string` | `varchar` |  |  | BOT_PRTP |
| `bot_lvcr` | `float` | `float` |  |  | BOT_LVCR |
| `bot_adlen` | `float` | `float` |  |  | BOT_ADLEN |
| `bot_pool` | `string` | `varchar` |  |  | BOT_POOL |
| `bot_changed` | `string` | `varchar` |  |  | BOT_CHANGED |
| `bot_display` | `string` | `varchar` |  |  | BOT_DISPLAY |
| `bot_updatecount` | `float` | `float` |  |  | BOT_UPDATECOUNT |
| `bot_created` | `timestamp` | `timestamp_ntz` |  |  | BOT_CREATED |
| `bot_updated` | `timestamp` | `timestamp_ntz` |  |  | BOT_UPDATED |
| `bot_cloned` | `string` | `varchar` |  |  | BOT_CLONED |
| `bot_function` | `string` | `varchar` | `PK` |  | BOT_FUNCTION |
| `bot_number` | `float` | `float` | `PK` |  | BOT_NUMBER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

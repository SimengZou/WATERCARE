# r5bvdwboilertexts

eam_R5BVDWBOILERTEXTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bvdwboilertexts` |
| **Materialization** | `incremental` |
| **Column count** | 10 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `bot_function` | `string` | `varchar` |  |  | BOT_FUNCTION |
| `bot_lang` | `string` | `varchar` |  |  | BOT_LANG |
| `bot_number` | `float` | `float` |  |  | BOT_NUMBER |
| `bot_text` | `string` | `varchar` |  |  | BOT_TEXT |
| `bot_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | BOT_LASTSAVED |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

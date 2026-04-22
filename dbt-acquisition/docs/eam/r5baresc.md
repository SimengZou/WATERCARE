# r5baresc

eam_R5BARESC

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5baresc` |
| **Materialization** | `incremental` |
| **Primary keys** | `bce_line`, `bce_escape`, `bce_column` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `bce_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | BCE_LASTSAVED |
| `bce_text2` | `string` | `varchar` |  |  | BCE_TEXT2 |
| `bce_line` | `float` | `float` | `PK` |  | BCE_LINE |
| `bce_escape` | `string` | `varchar` | `PK` |  | BCE_ESCAPE |
| `bce_updatecount` | `float` | `float` |  |  | BCE_UPDATECOUNT |
| `bce_text1` | `string` | `varchar` |  |  | BCE_TEXT1 |
| `bce_column` | `string` | `varchar` | `PK` |  | BCE_COLUMN |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

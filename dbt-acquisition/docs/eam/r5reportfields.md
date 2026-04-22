# r5reportfields

eam_R5REPORTFIELDS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reportfields` |
| **Materialization** | `incremental` |
| **Primary keys** | `rfl_function`, `rfl_line` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rfl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RFL_LASTSAVED |
| `rfl_line` | `float` | `float` | `PK` |  | RFL_LINE |
| `rfl_botnumber` | `float` | `float` |  |  | RFL_BOTNUMBER |
| `rfl_field` | `string` | `varchar` |  |  | RFL_FIELD |
| `rfl_showfield` | `string` | `varchar` |  |  | RFL_SHOWFIELD |
| `rfl_width` | `float` | `float` |  |  | RFL_WIDTH |
| `rfl_updatecount` | `float` | `float` |  |  | RFL_UPDATECOUNT |
| `rfl_updated` | `timestamp` | `timestamp_ntz` |  |  | RFL_UPDATED |
| `rfl_function` | `string` | `varchar` | `PK` |  | RFL_FUNCTION |
| `rfl_datatype` | `string` | `varchar` |  |  | RFL_DATATYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

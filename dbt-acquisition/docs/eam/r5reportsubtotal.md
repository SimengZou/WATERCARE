# r5reportsubtotal

eam_R5REPORTSUBTOTAL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reportsubtotal` |
| **Materialization** | `incremental` |
| **Primary keys** | `rst_function`, `rst_groupline`, `rst_line` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rst_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RST_LASTSAVED |
| `rst_groupline` | `float` | `float` | `PK` |  | RST_GROUPLINE |
| `rst_line` | `float` | `float` | `PK` |  | RST_LINE |
| `rst_botnumber` | `float` | `float` |  |  | RST_BOTNUMBER |
| `rst_datatype` | `string` | `varchar` |  |  | RST_DATATYPE |
| `rst_width` | `float` | `float` |  |  | RST_WIDTH |
| `rst_updatecount` | `float` | `float` |  |  | RST_UPDATECOUNT |
| `rst_updated` | `timestamp` | `timestamp_ntz` |  |  | RST_UPDATED |
| `rst_function` | `string` | `varchar` | `PK` |  | RST_FUNCTION |
| `rst_field` | `string` | `varchar` |  |  | RST_FIELD |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

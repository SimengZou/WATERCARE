# r5mailparameters

eam_R5MAILPARAMETERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mailparameters` |
| **Materialization** | `incremental` |
| **Primary keys** | `map_attribpk`, `map_parameter` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `map_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MAP_LASTSAVED |
| `map_template` | `string` | `varchar` |  |  | MAP_TEMPLATE |
| `map_column` | `string` | `varchar` |  |  | MAP_COLUMN |
| `map_updatecount` | `float` | `float` |  |  | MAP_UPDATECOUNT |
| `map_attribpk` | `string` | `varchar` | `PK` |  | MAP_ATTRIBPK |
| `map_reportparameter` | `float` | `float` |  |  | MAP_REPORTPARAMETER |
| `map_table` | `string` | `varchar` |  |  | MAP_TABLE |
| `map_parameter` | `float` | `float` | `PK` |  | MAP_PARAMETER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

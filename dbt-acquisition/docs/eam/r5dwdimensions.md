# r5dwdimensions

eam_R5DWDIMENSIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwdimensions` |
| **Materialization** | `incremental` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dim_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DIM_LASTSAVED |
| `dim_desc` | `string` | `varchar` |  |  | DIM_DESC |
| `dim_tabletype` | `string` | `varchar` |  |  | DIM_TABLETYPE |
| `dim_createkeysequence` | `string` | `varchar` |  |  | DIM_CREATEKEYSEQUENCE |
| `dim_surrogatekeylookuptbl` | `string` | `varchar` |  |  | DIM_SURROGATEKEYLOOKUPTBL |
| `dim_botnumber` | `float` | `float` |  |  | DIM_BOTNUMBER |
| `dim_table` | `string` | `varchar` |  |  | DIM_TABLE |
| `dim_keyfield` | `string` | `varchar` |  |  | DIM_KEYFIELD |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5shfpers

eam_R5SHFPERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5shfpers` |
| **Materialization** | `incremental` |
| **Primary keys** | `shp_shift`, `shp_person`, `shp_start`, `shp_end` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `shp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SHP_LASTSAVED |
| `shp_person` | `string` | `varchar` | `PK` |  | SHP_PERSON |
| `shp_end` | `timestamp` | `timestamp_ntz` | `PK` |  | SHP_END |
| `shp_updatecount` | `float` | `float` |  |  | SHP_UPDATECOUNT |
| `shp_shift` | `string` | `varchar` | `PK` |  | SHP_SHIFT |
| `shp_start` | `timestamp` | `timestamp_ntz` | `PK` |  | SHP_START |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

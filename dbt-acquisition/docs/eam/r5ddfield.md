# r5ddfield

eam_R5DDFIELD

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5ddfield` |
| **Materialization** | `incremental` |
| **Primary keys** | `ddf_fieldid` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ddf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DDF_LASTSAVED |
| `ddf_sourcename` | `string` | `varchar` |  |  | DDF_SOURCENAME |
| `ddf_datatype` | `string` | `varchar` |  |  | DDF_DATATYPE |
| `ddf_desc` | `string` | `varchar` |  |  | DDF_DESC |
| `ddf_updatecount` | `float` | `float` |  |  | DDF_UPDATECOUNT |
| `ddf_rentity` | `string` | `varchar` |  |  | DDF_RENTITY |
| `ddf_lvgrid` | `string` | `varchar` |  |  | DDF_LVGRID |
| `ddf_tablename` | `string` | `varchar` |  |  | DDF_TABLENAME |
| `ddf_fieldid` | `float` | `float` | `PK` | `unique` | DDF_FIELDID |
| `ddf_valuemapid` | `float` | `float` |  |  | DDF_VALUEMAPID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

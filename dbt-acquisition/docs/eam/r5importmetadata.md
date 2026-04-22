# r5importmetadata

eam_R5IMPORTMETADATA

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5importmetadata` |
| **Materialization** | `incremental` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `imd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | IMD_LASTSAVED |
| `imd_line` | `float` | `float` |  |  | IMD_LINE |
| `imd_metadata` | `string` | `varchar` |  |  | IMD_METADATA. Max length: 0 |
| `imd_sessionid` | `float` | `float` |  |  | IMD_SESSIONID |
| `imd_table` | `string` | `varchar` |  |  | IMD_TABLE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

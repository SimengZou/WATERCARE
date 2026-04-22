# r5dwdatamarts

eam_R5DWDATAMARTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwdatamarts` |
| **Materialization** | `incremental` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dmt_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DMT_LASTSAVED |
| `dmt_desc` | `string` | `varchar` |  |  | DMT_DESC |
| `dmt_tabletype` | `string` | `varchar` |  |  | DMT_TABLETYPE |
| `dmt_botnumber` | `float` | `float` |  |  | DMT_BOTNUMBER |
| `dmt_table` | `string` | `varchar` |  |  | DMT_TABLE |
| `dmt_grain` | `string` | `varchar` |  |  | DMT_GRAIN |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

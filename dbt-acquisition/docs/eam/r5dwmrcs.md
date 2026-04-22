# r5dwmrcs

eam_R5DWMRCS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwmrcs` |
| **Materialization** | `incremental` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zmr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZMR_LASTSAVED |
| `zmr_code` | `string` | `varchar` |  |  | ZMR_CODE |
| `zmr_classdesc` | `string` | `varchar` |  |  | ZMR_CLASSDESC |
| `zmr_classcode` | `string` | `varchar` |  |  | ZMR_CLASSCODE |
| `zmr_classorg` | `string` | `varchar` |  |  | ZMR_CLASSORG |
| `zmr_key` | `float` | `float` |  |  | ZMR_KEY |
| `zmr_desc` | `string` | `varchar` |  |  | ZMR_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5dwtrades

eam_R5DWTRADES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwtrades` |
| **Materialization** | `incremental` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ztr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZTR_LASTSAVED |
| `ztr_code` | `string` | `varchar` |  |  | ZTR_CODE |
| `ztr_classdesc` | `string` | `varchar` |  |  | ZTR_CLASSDESC |
| `ztr_classcode` | `string` | `varchar` |  |  | ZTR_CLASSCODE |
| `ztr_classorg` | `string` | `varchar` |  |  | ZTR_CLASSORG |
| `ztr_key` | `float` | `float` |  |  | ZTR_KEY |
| `ztr_desc` | `string` | `varchar` |  |  | ZTR_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

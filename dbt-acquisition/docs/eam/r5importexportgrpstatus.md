# r5importexportgrpstatus

eam_R5IMPORTEXPORTGRPSTATUS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5importexportgrpstatus` |
| **Materialization** | `incremental` |
| **Primary keys** | `img_sessionid`, `img_group` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `img_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | IMG_LASTSAVED |
| `img_group` | `string` | `varchar` | `PK` |  | IMG_GROUP |
| `img_status` | `string` | `varchar` |  |  | IMG_STATUS |
| `img_start` | `timestamp` | `timestamp_ntz` |  |  | IMG_START |
| `img_end` | `timestamp` | `timestamp_ntz` |  |  | IMG_END |
| `img_sessionid` | `float` | `float` | `PK` |  | IMG_SESSIONID |
| `img_processorder` | `float` | `float` |  |  | IMG_PROCESSORDER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

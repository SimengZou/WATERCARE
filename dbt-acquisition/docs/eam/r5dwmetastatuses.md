# r5dwmetastatuses

eam_R5DWMETASTATUSES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwmetastatuses` |
| **Materialization** | `incremental` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dms_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DMS_LASTSAVED |
| `dms_field` | `string` | `varchar` |  |  | DMS_FIELD |
| `dms_statusentity` | `string` | `varchar` |  |  | DMS_STATUSENTITY |
| `dms_statusentitybot` | `float` | `float` |  |  | DMS_STATUSENTITYBOT |
| `dms_entitybot` | `float` | `float` |  |  | DMS_ENTITYBOT |
| `dms_updated` | `timestamp` | `timestamp_ntz` |  |  | DMS_UPDATED |
| `dms_table` | `string` | `varchar` |  |  | DMS_TABLE |
| `dms_entity` | `string` | `varchar` |  |  | DMS_ENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

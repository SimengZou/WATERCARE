# r5userobjects

eam_R5USEROBJECTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userobjects` |
| **Materialization** | `incremental` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `uob_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UOB_LASTSAVED |
| `uob_objecttype` | `string` | `varchar` |  |  | UOB_OBJECTTYPE |
| `uob_obj_xtype` | `string` | `varchar` |  |  | UOB_OBJ_XTYPE |
| `uob_database` | `string` | `varchar` |  |  | UOB_DATABASE |
| `uob_objectname` | `string` | `varchar` |  |  | UOB_OBJECTNAME |
| `uob_objectsubtype` | `string` | `varchar` |  |  | UOB_OBJECTSUBTYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5productversionhistory

eam_R5PRODUCTVERSIONHISTORY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5productversionhistory` |
| **Materialization** | `incremental` |
| **Primary keys** | `pvh_productcode`, `pvh_changed` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pvh_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PVH_LASTSAVED |
| `pvh_changed` | `timestamp` | `timestamp_ntz` | `PK` |  | PVH_CHANGED |
| `pvh_desc` | `string` | `varchar` |  |  | PVH_DESC |
| `pvh_version` | `string` | `varchar` |  |  | PVH_VERSION |
| `pvh_patch` | `string` | `varchar` |  |  | PVH_PATCH |
| `pvh_updatecount` | `float` | `float` |  |  | PVH_UPDATECOUNT |
| `pvh_builddate` | `string` | `varchar` |  |  | PVH_BUILDDATE |
| `pvh_productcode` | `string` | `varchar` | `PK` |  | PVH_PRODUCTCODE |
| `pvh_build` | `string` | `varchar` |  |  | PVH_BUILD |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

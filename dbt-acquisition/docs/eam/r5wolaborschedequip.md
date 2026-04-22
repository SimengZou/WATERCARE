# r5wolaborschedequip

eam_R5WOLABORSCHEDEQUIP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wolaborschedequip` |
| **Materialization** | `incremental` |
| **Primary keys** | `wle_sessionid`, `wle_object`, `wle_object_org` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wle_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WLE_LASTSAVED |
| `wle_object` | `string` | `varchar` | `PK` |  | WLE_OBJECT |
| `wle_select` | `string` | `varchar` |  |  | WLE_SELECT |
| `wle_updatecount` | `float` | `float` |  |  | WLE_UPDATECOUNT |
| `wle_sessionid` | `float` | `float` | `PK` |  | WLE_SESSIONID |
| `wle_object_org` | `string` | `varchar` | `PK` |  | WLE_OBJECT_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5actclasses

eam_R5ACTCLASSES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5actclasses` |
| **Materialization** | `incremental` |
| **Primary keys** | `acl_class`, `acl_action` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `acl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ACL_LASTSAVED |
| `acl_updatecount` | `float` | `float` |  |  | ACL_UPDATECOUNT |
| `acl_action` | `string` | `varchar` | `PK` |  | ACL_ACTION |
| `acl_updated` | `timestamp` | `timestamp_ntz` |  |  | ACL_UPDATED |
| `acl_class` | `string` | `varchar` | `PK` |  | ACL_CLASS |
| `acl_created` | `timestamp` | `timestamp_ntz` |  |  | ACL_CREATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

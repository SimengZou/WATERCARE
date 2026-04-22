# r5routes

eam_R5ROUTES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5routes` |
| **Materialization** | `incremental` |
| **Primary keys** | `rou_code`, `rou_revision` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rou_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ROU_LASTSAVED |
| `rou_desc` | `string` | `varchar` |  |  | ROU_DESC |
| `rou_category` | `string` | `varchar` |  |  | ROU_CATEGORY |
| `rou_template` | `string` | `varchar` |  |  | ROU_TEMPLATE |
| `rou_class` | `string` | `varchar` |  |  | ROU_CLASS |
| `rou_revision` | `float` | `float` | `PK` |  | ROU_REVISION |
| `rou_revstatus` | `string` | `varchar` |  |  | ROU_REVSTATUS |
| `rou_org` | `string` | `varchar` |  |  | ROU_ORG |
| `rou_class_org` | `string` | `varchar` |  |  | ROU_CLASS_ORG |
| `rou_updatecount` | `float` | `float` |  |  | ROU_UPDATECOUNT |
| `rou_linkgiswo` | `string` | `varchar` |  |  | ROU_LINKGISWO |
| `rou_code` | `string` | `varchar` | `PK` |  | ROU_CODE |
| `rou_revrstatus` | `string` | `varchar` |  |  | ROU_REVRSTATUS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

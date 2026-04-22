# r5pmforecastpreview

eam_R5PMFORECASTPREVIEW

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pmforecastpreview` |
| **Materialization** | `incremental` |
| **Primary keys** | `pfv_sessionid`, `pfv_object`, `pfv_object_org` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pfv_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PFV_LASTSAVED |
| `pfv_select` | `string` | `varchar` |  |  | PFV_SELECT |
| `pfv_object` | `string` | `varchar` | `PK` |  | PFV_OBJECT |
| `pfv_parent` | `string` | `varchar` |  |  | PFV_PARENT |
| `pfv_parent_org` | `string` | `varchar` |  |  | PFV_PARENT_ORG |
| `pfv_updatecount` | `float` | `float` |  |  | PFV_UPDATECOUNT |
| `pfv_sessionid` | `float` | `float` | `PK` |  | PFV_SESSIONID |
| `pfv_object_org` | `string` | `varchar` | `PK` |  | PFV_OBJECT_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

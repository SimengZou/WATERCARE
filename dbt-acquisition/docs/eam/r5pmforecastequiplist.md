# r5pmforecastequiplist

eam_R5PMFORECASTEQUIPLIST

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pmforecastequiplist` |
| **Materialization** | `incremental` |
| **Primary keys** | `pfl_sessionid`, `pfl_object`, `pfl_object_org` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pfl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PFL_LASTSAVED |
| `pfl_object` | `string` | `varchar` | `PK` |  | PFL_OBJECT |
| `pfl_object_org` | `string` | `varchar` | `PK` |  | PFL_OBJECT_ORG |
| `pfl_select` | `string` | `varchar` |  |  | PFL_SELECT |
| `pfl_parent_org` | `string` | `varchar` |  |  | PFL_PARENT_ORG |
| `pfl_updatecount` | `float` | `float` |  |  | PFL_UPDATECOUNT |
| `pfl_sessionid` | `float` | `float` | `PK` |  | PFL_SESSIONID |
| `pfl_parent` | `string` | `varchar` |  |  | PFL_PARENT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

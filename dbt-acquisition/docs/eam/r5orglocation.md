# r5orglocation

eam_R5ORGLOCATION

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5orglocation` |
| **Materialization** | `incremental` |
| **Primary keys** | `ogl_org`, `ogl_bodgroup` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ogl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OGL_LASTSAVED |
| `ogl_bodgroup` | `string` | `varchar` | `PK` |  | OGL_BODGROUP |
| `ogl_updatecount` | `float` | `float` |  |  | OGL_UPDATECOUNT |
| `ogl_org` | `string` | `varchar` | `PK` |  | OGL_ORG |
| `ogl_enterpriselocation` | `string` | `varchar` |  |  | OGL_ENTERPRISELOCATION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

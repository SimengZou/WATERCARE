# r5conclauses

eam_R5CONCLAUSES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5conclauses` |
| **Materialization** | `incremental` |
| **Primary keys** | `ccl_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ccl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CCL_LASTSAVED |
| `ccl_class` | `string` | `varchar` |  |  | CCL_CLASS |
| `ccl_parent` | `string` | `varchar` |  |  | CCL_PARENT |
| `ccl_line` | `float` | `float` |  |  | CCL_LINE |
| `ccl_system` | `string` | `varchar` |  |  | CCL_SYSTEM |
| `ccl_code` | `string` | `varchar` | `PK` | `unique` | CCL_CODE |
| `ccl_class_org` | `string` | `varchar` |  |  | CCL_CLASS_ORG |
| `ccl_updatecount` | `float` | `float` |  |  | CCL_UPDATECOUNT |
| `ccl_notused` | `string` | `varchar` |  |  | CCL_NOTUSED |
| `ccl_desc` | `string` | `varchar` |  |  | CCL_DESC |
| `ccl_org` | `string` | `varchar` |  |  | CCL_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5schedgroups

eam_R5SCHEDGROUPS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5schedgroups` |
| **Materialization** | `incremental` |
| **Primary keys** | `scg_code` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `scg_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SCG_LASTSAVED |
| `scg_class` | `string` | `varchar` |  |  | SCG_CLASS |
| `scg_org` | `string` | `varchar` |  |  | SCG_ORG |
| `scg_class_org` | `string` | `varchar` |  |  | SCG_CLASS_ORG |
| `scg_code` | `string` | `varchar` | `PK` | `unique` | SCG_CODE |
| `scg_notused` | `string` | `varchar` |  |  | SCG_NOTUSED |
| `scg_profilepicture` | `string` | `varchar` |  |  | SCG_PROFILEPICTURE |
| `scg_desc` | `string` | `varchar` |  |  | SCG_DESC |
| `scg_updatecount` | `float` | `float` |  |  | SCG_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

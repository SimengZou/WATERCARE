# r5structures

eam_R5STRUCTURES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5structures` |
| **Materialization** | `incremental` |
| **Primary keys** | `stc_child`, `stc_child_org`, `stc_parent`, `stc_parent_org` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `stc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | STC_LASTSAVED |
| `stc_child` | `string` | `varchar` | `PK` |  | STC_CHILD |
| `stc_parenttype` | `string` | `varchar` |  |  | STC_PARENTTYPE |
| `stc_parentrtype` | `string` | `varchar` |  |  | STC_PARENTRTYPE |
| `stc_parent` | `string` | `varchar` | `PK` |  | STC_PARENT |
| `stc_rolldown` | `string` | `varchar` |  |  | STC_ROLLDOWN |
| `stc_rollup` | `string` | `varchar` |  |  | STC_ROLLUP |
| `stc_equivalent` | `string` | `varchar` |  |  | STC_EQUIVALENT |
| `stc_downtime` | `string` | `varchar` |  |  | STC_DOWNTIME |
| `stc_child_org` | `string` | `varchar` | `PK` |  | STC_CHILD_ORG |
| `stc_parent_org` | `string` | `varchar` | `PK` |  | STC_PARENT_ORG |
| `stc_updatecount` | `float` | `float` |  |  | STC_UPDATECOUNT |
| `stc_updated` | `timestamp` | `timestamp_ntz` |  |  | STC_UPDATED |
| `stc_sequence` | `float` | `float` |  |  | STC_SEQUENCE |
| `stc_mnbdefinition` | `string` | `varchar` |  |  | STC_MNBDEFINITION |
| `stc_childtype` | `string` | `varchar` |  |  | STC_CHILDTYPE |
| `stc_childrtype` | `string` | `varchar` |  |  | STC_CHILDRTYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

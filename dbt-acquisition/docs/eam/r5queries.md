# r5queries

eam_R5QUERIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5queries` |
| **Materialization** | `incremental` |
| **Primary keys** | `que_code` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `que_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | QUE_LASTSAVED |
| `que_text` | `string` | `varchar` |  |  | QUE_TEXT |
| `que_normal` | `string` | `varchar` |  |  | QUE_NORMAL |
| `que_asset` | `string` | `varchar` |  |  | QUE_ASSET |
| `que_inbox` | `string` | `varchar` |  |  | QUE_INBOX |
| `que_kpi` | `string` | `varchar` |  |  | QUE_KPI |
| `que_updatecount` | `float` | `float` |  |  | QUE_UPDATECOUNT |
| `que_updated` | `timestamp` | `timestamp_ntz` |  |  | QUE_UPDATED |
| `que_chart` | `string` | `varchar` |  |  | QUE_CHART |
| `que_desc` | `string` | `varchar` |  |  | QUE_DESC |
| `que_note` | `string` | `varchar` |  |  | QUE_NOTE |
| `que_equipmentranking` | `string` | `varchar` |  |  | QUE_EQUIPMENTRANKING |
| `que_code` | `string` | `varchar` | `PK` | `unique` | QUE_CODE |
| `que_lookup` | `string` | `varchar` |  |  | QUE_LOOKUP |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

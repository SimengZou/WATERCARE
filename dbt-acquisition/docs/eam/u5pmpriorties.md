# u5pmpriorties

eam_U5PMPRIORTIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5pmpriorties` |
| **Materialization** | `incremental` |
| **Primary keys** | `pmp_cycle_length`, `pmp_cycle_uom` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `pmp_cycle_uom` | `string` | `varchar` | `PK` |  | PMP_CYCLE_UOM |
| `pmp_cycle_text` | `string` | `varchar` |  |  | PMP_CYCLE_TEXT |
| `pmp_release_days` | `float` | `float` |  |  | PMP_RELEASE_DAYS |
| `pmp_priority` | `string` | `varchar` |  |  | PMP_PRIORITY |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `pmp_cycle_length` | `float` | `float` | `PK` |  | PMP_CYCLE_LENGTH |
| `pmp_complete_days` | `float` | `float` |  |  | PMP_COMPLETE_DAYS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

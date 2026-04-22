# r5extensibleframework

eam_R5EXTENSIBLEFRAMEWORK

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5extensibleframework` |
| **Materialization** | `incremental` |
| **Primary keys** | `efr_name` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `efr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EFR_LASTSAVED |
| `efr_sourcecode` | `string` | `varchar` |  |  | EFR_SOURCECODE. Max length: 0 |
| `efr_userfunction` | `string` | `varchar` |  |  | EFR_USERFUNCTION |
| `efr_updatecount` | `float` | `float` |  |  | EFR_UPDATECOUNT |
| `efr_name` | `string` | `varchar` | `PK` | `unique` | EFR_NAME |
| `efr_active` | `string` | `varchar` |  |  | EFR_ACTIVE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

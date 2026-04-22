# r5entappheader

eam_R5ENTAPPHEADER

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5entappheader` |
| **Materialization** | `incremental` |
| **Primary keys** | `eah_pk` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `eah_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EAH_LASTSAVED |
| `eah_rentity` | `string` | `varchar` |  |  | EAH_RENTITY |
| `eah_entity` | `string` | `varchar` |  |  | EAH_ENTITY |
| `eah_code` | `string` | `varchar` |  |  | EAH_CODE |
| `eah_revision` | `float` | `float` |  |  | EAH_REVISION |
| `eah_appdate` | `timestamp` | `timestamp_ntz` |  |  | EAH_APPDATE |
| `eah_user` | `string` | `varchar` |  |  | EAH_USER |
| `eah_date` | `timestamp` | `timestamp_ntz` |  |  | EAH_DATE |
| `eah_reason` | `string` | `varchar` |  |  | EAH_REASON |
| `eah_updatecount` | `float` | `float` |  |  | EAH_UPDATECOUNT |
| `eah_pk` | `float` | `float` | `PK` | `unique` | EAH_PK |
| `eah_applist` | `string` | `varchar` |  |  | EAH_APPLIST |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5exchrates

eam_R5EXCHRATES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5exchrates` |
| **Materialization** | `incremental` |
| **Primary keys** | `crr_code` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `crr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CRR_LASTSAVED |
| `crr_exch` | `float` | `float` |  |  | CRR_EXCH |
| `crr_start` | `timestamp` | `timestamp_ntz` |  |  | CRR_START |
| `crr_end` | `timestamp` | `timestamp_ntz` |  |  | CRR_END |
| `crr_active` | `string` | `varchar` |  |  | CRR_ACTIVE |
| `crr_sourcecode` | `string` | `varchar` |  |  | CRR_SOURCECODE |
| `crr_interface` | `timestamp` | `timestamp_ntz` |  |  | CRR_INTERFACE |
| `crr_orgcurr` | `string` | `varchar` |  |  | CRR_ORGCURR |
| `crr_updatecount` | `float` | `float` |  |  | CRR_UPDATECOUNT |
| `crr_code` | `string` | `varchar` | `PK` | `unique` | CRR_CODE |
| `crr_curr` | `string` | `varchar` |  |  | CRR_CURR |
| `crr_sourcesystem` | `string` | `varchar` |  |  | CRR_SOURCESYSTEM |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5preferences

eam_R5PREFERENCES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5preferences` |
| **Materialization** | `incremental` |
| **Primary keys** | `prf_code` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `prf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PRF_LASTSAVED |
| `prf_nr` | `float` | `float` |  |  | PRF_NR |
| `prf_updatecount` | `float` | `float` |  |  | PRF_UPDATECOUNT |
| `prf_code` | `string` | `varchar` | `PK` | `unique` | PRF_CODE |
| `prf_default` | `string` | `varchar` |  |  | PRF_DEFAULT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

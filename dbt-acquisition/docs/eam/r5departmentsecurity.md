# r5departmentsecurity

eam_R5DEPARTMENTSECURITY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5departmentsecurity` |
| **Materialization** | `incremental` |
| **Primary keys** | `dse_user`, `dse_mrc` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dse_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DSE_LASTSAVED |
| `dse_mrc` | `string` | `varchar` | `PK` |  | DSE_MRC |
| `dse_readonly` | `string` | `varchar` |  |  | DSE_READONLY |
| `dse_updated` | `timestamp` | `timestamp_ntz` |  |  | DSE_UPDATED |
| `dse_user` | `string` | `varchar` | `PK` |  | DSE_USER |
| `dse_updatecount` | `float` | `float` |  |  | DSE_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

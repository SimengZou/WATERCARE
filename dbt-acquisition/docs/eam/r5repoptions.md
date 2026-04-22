# r5repoptions

eam_R5REPOPTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5repoptions` |
| **Materialization** | `incremental` |
| **Primary keys** | `rop_function`, `rop_parameter`, `rop_seqno` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rop_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ROP_LASTSAVED |
| `rop_parameter` | `string` | `varchar` | `PK` |  | ROP_PARAMETER |
| `rop_seqno` | `float` | `float` | `PK` |  | ROP_SEQNO |
| `rop_updatecount` | `float` | `float` |  |  | ROP_UPDATECOUNT |
| `rop_updated` | `timestamp` | `timestamp_ntz` |  |  | ROP_UPDATED |
| `rop_mekey` | `string` | `varchar` |  |  | ROP_MEKEY |
| `rop_function` | `string` | `varchar` | `PK` |  | ROP_FUNCTION |
| `rop_value` | `string` | `varchar` |  |  | ROP_VALUE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

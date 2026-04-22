# r5userlpftypes

eam_R5USERLPFTYPES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userlpftypes` |
| **Materialization** | `incremental` |
| **Primary keys** | `lpt_linearpreference`, `lpt_type` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lpt_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LPT_LASTSAVED |
| `lpt_type` | `string` | `varchar` | `PK` |  | LPT_TYPE |
| `lpt_sequence` | `float` | `float` |  |  | LPT_SEQUENCE |
| `lpt_updatecount` | `float` | `float` |  |  | LPT_UPDATECOUNT |
| `lpt_linearpreference` | `string` | `varchar` | `PK` |  | LPT_LINEARPREFERENCE |
| `lpt_rtype` | `string` | `varchar` |  |  | LPT_RTYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

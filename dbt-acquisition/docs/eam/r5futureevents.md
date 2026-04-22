# r5futureevents

eam_R5FUTUREEVENTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5futureevents` |
| **Materialization** | `incremental` |
| **Primary keys** | `fut_event`, `fut_seqno` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `fut_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FUT_LASTSAVED |
| `fut_seqno` | `float` | `float` | `PK` |  | FUT_SEQNO |
| `fut_updatecount` | `float` | `float` |  |  | FUT_UPDATECOUNT |
| `fut_mp_seq` | `float` | `float` |  |  | FUT_MP_SEQ |
| `fut_event` | `string` | `varchar` | `PK` |  | FUT_EVENT |
| `fut_date` | `timestamp` | `timestamp_ntz` |  |  | FUT_DATE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

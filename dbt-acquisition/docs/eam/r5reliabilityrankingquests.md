# r5reliabilityrankingquests

eam_R5RELIABILITYRANKINGQUESTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reliabilityrankingquests` |
| **Materialization** | `incremental` |
| **Primary keys** | `rrq_levelpk`, `rrq_lang` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rrq_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RRQ_LASTSAVED |
| `rrq_lang` | `string` | `varchar` | `PK` |  | RRQ_LANG |
| `rrq_trans` | `string` | `varchar` |  |  | RRQ_TRANS |
| `rrq_updatecount` | `float` | `float` |  |  | RRQ_UPDATECOUNT |
| `rrq_levelpk` | `string` | `varchar` | `PK` |  | RRQ_LEVELPK |
| `rrq_question` | `string` | `varchar` |  |  | RRQ_QUESTION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

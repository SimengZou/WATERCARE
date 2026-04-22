# r5wspreturnprompts

eam_R5WSPRETURNPROMPTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wspreturnprompts` |
| **Materialization** | `incremental` |
| **Primary keys** | `wpr_wspcode`, `wpr_sourceseq`, `wpr_targetseq` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wpr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WPR_LASTSAVED |
| `wpr_sourceseq` | `float` | `float` | `PK` |  | WPR_SOURCESEQ |
| `wpr_targetseq` | `float` | `float` | `PK` |  | WPR_TARGETSEQ |
| `wpr_updated` | `timestamp` | `timestamp_ntz` |  |  | WPR_UPDATED |
| `wpr_updatecount` | `float` | `float` |  |  | WPR_UPDATECOUNT |
| `wpr_mobilequerycode` | `string` | `varchar` |  |  | WPR_MOBILEQUERYCODE |
| `wpr_wspcode` | `string` | `varchar` | `PK` |  | WPR_WSPCODE |
| `wpr_querycode` | `string` | `varchar` |  |  | WPR_QUERYCODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

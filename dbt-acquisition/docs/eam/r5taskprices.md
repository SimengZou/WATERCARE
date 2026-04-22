# r5taskprices

eam_R5TASKPRICES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5taskprices` |
| **Materialization** | `incremental` |
| **Primary keys** | `tpr_task`, `tpr_revision`, `tpr_org` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tpr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TPR_LASTSAVED |
| `tpr_org` | `string` | `varchar` | `PK` |  | TPR_ORG |
| `tpr_task` | `string` | `varchar` | `PK` |  | TPR_TASK |
| `tpr_updatecount` | `float` | `float` |  |  | TPR_UPDATECOUNT |
| `tpr_revision` | `float` | `float` | `PK` |  | TPR_REVISION |
| `tpr_price` | `float` | `float` |  |  | TPR_PRICE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

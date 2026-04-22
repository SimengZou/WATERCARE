# r5queryfield

eam_R5QUERYFIELD

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5queryfield` |
| **Materialization** | `incremental` |
| **Primary keys** | `dqf_fieldid`, `dqf_ddspyid`, `dqf_viewtype` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dqf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DQF_LASTSAVED |
| `dqf_fieldid` | `float` | `float` | `PK` |  | DQF_FIELDID |
| `dqf_columnwidth` | `string` | `varchar` |  |  | DQF_COLUMNWIDTH |
| `dqf_updatecount` | `float` | `float` |  |  | DQF_UPDATECOUNT |
| `dqf_viewtype` | `string` | `varchar` | `PK` |  | DQF_VIEWTYPE |
| `dqf_updated` | `timestamp` | `timestamp_ntz` |  |  | DQF_UPDATED |
| `dqf_ddspyid` | `float` | `float` | `PK` |  | DQF_DDSPYID |
| `dqf_columnorder` | `float` | `float` |  |  | DQF_COLUMNORDER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

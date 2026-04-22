# r5dlvwsprompts

eam_R5DLVWSPROMPTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dlvwsprompts` |
| **Materialization** | `incremental` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wst_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WST_LASTSAVED |
| `wst_desc` | `string` | `varchar` |  |  | WST_DESC |
| `wst_notused` | `string` | `varchar` |  |  | WST_NOTUSED |
| `wst_updatecount` | `float` | `float` |  |  | WST_UPDATECOUNT |
| `wst_function` | `string` | `varchar` |  |  | WST_FUNCTION |
| `wst_tab` | `string` | `varchar` |  |  | WST_TAB |
| `wst_system` | `string` | `varchar` |  |  | WST_SYSTEM |
| `wst_code` | `string` | `varchar` |  |  | WST_CODE |
| `wst_updated` | `timestamp` | `timestamp_ntz` |  |  | WST_UPDATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

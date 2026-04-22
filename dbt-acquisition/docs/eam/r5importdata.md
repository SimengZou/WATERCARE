# r5importdata

eam_R5IMPORTDATA

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5importdata` |
| **Materialization** | `incremental` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ipd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | IPD_LASTSAVED |
| `ipd_line` | `float` | `float` |  |  | IPD_LINE |
| `ipd_status` | `string` | `varchar` |  |  | IPD_STATUS |
| `ipd_message` | `string` | `varchar` |  |  | IPD_MESSAGE |
| `ipd_data` | `string` | `varchar` |  |  | IPD_DATA. Max length: 0 |
| `ipd_decoded` | `string` | `varchar` |  |  | IPD_DECODED. Max length: 0 |
| `ipd_updated` | `timestamp` | `timestamp_ntz` |  |  | IPD_UPDATED |
| `ipd_sessionid` | `float` | `float` |  |  | IPD_SESSIONID |
| `ipd_table` | `string` | `varchar` |  |  | IPD_TABLE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

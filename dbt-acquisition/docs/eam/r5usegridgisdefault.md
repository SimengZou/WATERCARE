# r5usegridgisdefault

eam_R5USEGRIDGISDEFAULT

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5usegridgisdefault` |
| **Materialization** | `incremental` |
| **Primary keys** | `ugd_gridid`, `ugd_userid`, `ugd_gisservice`, `ugd_gislayer` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ugd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UGD_LASTSAVED |
| `ugd_userid` | `string` | `varchar` | `PK` |  | UGD_USERID |
| `ugd_gisservice` | `string` | `varchar` | `PK` |  | UGD_GISSERVICE |
| `ugd_dataspyid` | `float` | `float` |  |  | UGD_DATASPYID |
| `ugd_updatecount` | `float` | `float` |  |  | UGD_UPDATECOUNT |
| `ugd_gridid` | `float` | `float` | `PK` |  | UGD_GRIDID |
| `ugd_gislayer` | `string` | `varchar` | `PK` |  | UGD_GISLAYER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

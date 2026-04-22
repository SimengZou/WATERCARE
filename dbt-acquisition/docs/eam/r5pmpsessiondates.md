# r5pmpsessiondates

eam_R5PMPSESSIONDATES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pmpsessiondates` |
| **Materialization** | `incremental` |
| **Primary keys** | `ppd_line`, `ppd_ppspk`, `ppd_duedate` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ppd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PPD_LASTSAVED |
| `ppd_ppspk` | `float` | `float` | `PK` |  | PPD_PPSPK |
| `ppd_duedate` | `timestamp` | `timestamp_ntz` | `PK` |  | PPD_DUEDATE |
| `ppd_iscaldate` | `string` | `varchar` |  |  | PPD_ISCALDATE |
| `ppd_isprojected` | `string` | `varchar` |  |  | PPD_ISPROJECTED |
| `ppd_updatecount` | `float` | `float` |  |  | PPD_UPDATECOUNT |
| `ppd_line` | `float` | `float` | `PK` |  | PPD_LINE |
| `ppd_workorder` | `string` | `varchar` |  |  | PPD_WORKORDER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

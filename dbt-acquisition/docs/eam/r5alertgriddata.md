# r5alertgriddata

eam_R5ALERTGRIDDATA

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alertgriddata` |
| **Materialization** | `incremental` |
| **Primary keys** | `agd_pk` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `agd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AGD_LASTSAVED |
| `agd_alert` | `string` | `varchar` |  |  | AGD_ALERT |
| `agd_gridid` | `float` | `float` |  |  | AGD_GRIDID |
| `agd_dataspyid` | `float` | `float` |  |  | AGD_DATASPYID |
| `agd_recipient` | `string` | `varchar` |  |  | AGD_RECIPIENT |
| `agd_description` | `string` | `varchar` |  |  | AGD_DESCRIPTION |
| `agd_data` | `string` | `varchar` |  |  | AGD_DATA. Max length: 0 |
| `agd_pk` | `string` | `varchar` | `PK` | `unique` | AGD_PK |
| `agd_date` | `timestamp` | `timestamp_ntz` |  |  | AGD_DATE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

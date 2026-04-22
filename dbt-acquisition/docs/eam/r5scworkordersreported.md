# r5scworkordersreported

eam_R5SCWORKORDERSREPORTED

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5scworkordersreported` |
| **Materialization** | `incremental` |
| **Primary keys** | `cwr_org`, `cwr_mrc`, `cwr_date` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cwr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CWR_LASTSAVED |
| `cwr_date` | `timestamp` | `timestamp_ntz` | `PK` |  | CWR_DATE |
| `cwr_org` | `string` | `varchar` | `PK` |  | CWR_ORG |
| `cwr_mrc` | `string` | `varchar` | `PK` |  | CWR_MRC |
| `cwr_wosreported` | `float` | `float` |  |  | CWR_WOSREPORTED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

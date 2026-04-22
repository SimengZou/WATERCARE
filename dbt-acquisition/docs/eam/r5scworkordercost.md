# r5scworkordercost

eam_R5SCWORKORDERCOST

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5scworkordercost` |
| **Materialization** | `incremental` |
| **Primary keys** | `cws_org`, `cws_jobtype`, `cws_date` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cws_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CWS_LASTSAVED |
| `cws_jobtype` | `string` | `varchar` | `PK` |  | CWS_JOBTYPE |
| `cws_cost` | `float` | `float` |  |  | CWS_COST |
| `cws_costdefcurr` | `float` | `float` |  |  | CWS_COSTDEFCURR |
| `cws_org` | `string` | `varchar` | `PK` |  | CWS_ORG |
| `cws_date` | `timestamp` | `timestamp_ntz` | `PK` |  | CWS_DATE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

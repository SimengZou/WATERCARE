# r5custom

eam_R5CUSTOM

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5custom` |
| **Materialization** | `incremental` |
| **Primary keys** | `cus_date`, `cus_title`, `cus_objects` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cus_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CUS_LASTSAVED |
| `cus_applied` | `timestamp` | `timestamp_ntz` |  |  | CUS_APPLIED |
| `cus_title` | `string` | `varchar` | `PK` |  | CUS_TITLE |
| `cus_desc` | `string` | `varchar` |  |  | CUS_DESC |
| `cus_updatecount` | `float` | `float` |  |  | CUS_UPDATECOUNT |
| `cus_date` | `timestamp` | `timestamp_ntz` | `PK` |  | CUS_DATE |
| `cus_objects` | `string` | `varchar` | `PK` |  | CUS_OBJECTS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

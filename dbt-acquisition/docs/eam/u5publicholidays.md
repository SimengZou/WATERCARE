# u5publicholidays

eam_U5PUBLICHOLIDAYS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5publicholidays` |
| **Materialization** | `incremental` |
| **Primary keys** | `hly_id` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `hly_org` | `string` | `varchar` |  |  | HLY_ORG |
| `hly_concode` | `string` | `varchar` |  |  | HLY_CONCODE |
| `hly_year` | `string` | `varchar` |  |  | HLY_YEAR |
| `hly_date` | `timestamp` | `timestamp_ntz` |  |  | HLY_DATE |
| `hly_name` | `string` | `varchar` |  |  | HLY_NAME |
| `auto` | `float` | `float` |  |  | AUTO |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `hly_id` | `string` | `varchar` | `PK` | `unique` | HLY_ID |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

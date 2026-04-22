# u5assetclass

eam_U5ASSETCLASS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5assetclass` |
| **Materialization** | `incremental` |
| **Primary keys** | `ascl_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `ascl_code` | `string` | `varchar` | `PK` | `unique` | ASCL_CODE |
| `ascl_funcgroup` | `string` | `varchar` |  |  | ASCL_FUNCGROUP |
| `ascl_ipscomptype` | `float` | `float` |  |  | ASCL_IPSCOMPTYPE |
| `ascl_notused` | `string` | `varchar` |  |  | ASCL_NOTUSED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `description` | `string` | `varchar` |  |  | DESCRIPTION |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

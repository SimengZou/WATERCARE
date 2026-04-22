# u5ciclaims

eam_U5CICLAIMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5ciclaims` |
| **Materialization** | `incremental` |
| **Primary keys** | `cli_transid` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `cli_contractor` | `string` | `varchar` |  |  | CLI_CONTRACTOR |
| `cli_lastitem` | `string` | `varchar` |  |  | CLI_LASTITEM |
| `cli_cost` | `string` | `varchar` |  |  | CLI_COST. Max length: 0 |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `cli_transid` | `string` | `varchar` | `PK` | `unique` | CLI_TRANSID |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `cli_event` | `string` | `varchar` |  |  | CLI_EVENT |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

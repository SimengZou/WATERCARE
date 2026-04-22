# u5jobprresptime

eam_U5JOBPRRESPTIME

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5jobprresptime` |
| **Materialization** | `incremental` |
| **Primary keys** | `jbp_wopriority`, `jbp_wotype`, `jbp_contractor`, `jbp_organization` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `jbp_wotype` | `string` | `varchar` | `PK` |  | JBP_WOTYPE |
| `jbp_contractor` | `string` | `varchar` | `PK` |  | JBP_CONTRACTOR |
| `jbp_woresponsetime` | `float` | `float` |  |  | JBP_WORESPONSETIME |
| `jbp_wocompletiontime` | `float` | `float` |  |  | JBP_WOCOMPLETIONTIME |
| `jbp_woresolutiontime` | `float` | `float` |  |  | JBP_WORESOLUTIONTIME |
| `jbp_businessdays` | `string` | `varchar` |  |  | JBP_BUSINESSDAYS |
| `jbp_organization` | `string` | `varchar` | `PK` |  | JBP_ORGANIZATION |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `jbp_wopriority` | `string` | `varchar` | `PK` |  | JBP_WOPRIORITY |
| `jbp_wopriority_desc` | `string` | `varchar` |  |  | JBP_WOPRIORITY_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

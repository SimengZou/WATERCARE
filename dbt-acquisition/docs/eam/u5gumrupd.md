# u5gumrupd

eam_U5GUMRUPD

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5gumrupd` |
| **Materialization** | `incremental` |
| **Primary keys** | `mru_transid` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `mru_event` | `string` | `varchar` |  |  | MRU_EVENT |
| `mru_contractorcode` | `string` | `varchar` |  |  | MRU_CONTRACTORCODE |
| `mru_updatetype` | `string` | `varchar` |  |  | MRU_UPDATETYPE |
| `mru_updatedate` | `timestamp` | `timestamp_ntz` |  |  | MRU_UPDATEDATE |
| `mru_comments` | `string` | `varchar` |  |  | MRU_COMMENTS. Max length: 0 |
| `mru_reason` | `string` | `varchar` |  |  | MRU_REASON |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `mru_transid` | `string` | `varchar` | `PK` | `unique` | MRU_TRANSID |
| `mru_number` | `string` | `varchar` |  |  | MRU_NUMBER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

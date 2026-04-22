# r5importexportstatus

eam_R5IMPORTEXPORTSTATUS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5importexportstatus` |
| **Materialization** | `incremental` |
| **Primary keys** | `ies_sessionid` |
| **Column count** | 25 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ies_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | IES_LASTSAVED |
| `ies_type` | `string` | `varchar` |  |  | IES_TYPE |
| `ies_status` | `string` | `varchar` |  |  | IES_STATUS |
| `ies_filelocation` | `string` | `varchar` |  |  | IES_FILELOCATION |
| `ies_emailsent` | `string` | `varchar` |  |  | IES_EMAILSENT |
| `ies_filesent` | `string` | `varchar` |  |  | IES_FILESENT |
| `ies_esttimetostart` | `timestamp` | `timestamp_ntz` |  |  | IES_ESTTIMETOSTART |
| `ies_esttimetoend` | `timestamp` | `timestamp_ntz` |  |  | IES_ESTTIMETOEND |
| `ies_datecreated` | `timestamp` | `timestamp_ntz` |  |  | IES_DATECREATED |
| `ies_updatecount` | `float` | `float` |  |  | IES_UPDATECOUNT |
| `ies_started` | `timestamp` | `timestamp_ntz` |  |  | IES_STARTED |
| `ies_completed` | `timestamp` | `timestamp_ntz` |  |  | IES_COMPLETED |
| `ies_recordcount` | `float` | `float` |  |  | IES_RECORDCOUNT |
| `ies_email` | `string` | `varchar` |  |  | IES_EMAIL |
| `ies_desc` | `string` | `varchar` |  |  | IES_DESC |
| `ies_includeemail` | `string` | `varchar` |  |  | IES_INCLUDEEMAIL |
| `ies_preview` | `string` | `varchar` |  |  | IES_PREVIEW |
| `ies_userid` | `string` | `varchar` |  |  | IES_USERID |
| `ies_sessionid` | `float` | `float` | `PK` | `unique` | IES_SESSIONID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5languages

eam_R5LANGUAGES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5languages` |
| **Materialization** | `incremental` |
| **Primary keys** | `lan_code` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lan_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LAN_LASTSAVED |
| `lan_class` | `string` | `varchar` |  |  | LAN_CLASS |
| `lan_txttype` | `string` | `varchar` |  |  | LAN_TXTTYPE |
| `lan_class_org` | `string` | `varchar` |  |  | LAN_CLASS_ORG |
| `lan_updatecount` | `float` | `float` |  |  | LAN_UPDATECOUNT |
| `lan_rstatus` | `string` | `varchar` |  |  | LAN_RSTATUS |
| `lan_lastcreated` | `timestamp` | `timestamp_ntz` |  |  | LAN_LASTCREATED |
| `lan_processstart` | `timestamp` | `timestamp_ntz` |  |  | LAN_PROCESSSTART |
| `lan_processend` | `timestamp` | `timestamp_ntz` |  |  | LAN_PROCESSEND |
| `lan_installed` | `string` | `varchar` |  |  | LAN_INSTALLED |
| `lan_notused` | `string` | `varchar` |  |  | LAN_NOTUSED |
| `lan_available` | `string` | `varchar` |  |  | LAN_AVAILABLE |
| `lan_errormessage` | `string` | `varchar` |  |  | LAN_ERRORMESSAGE |
| `lan_code` | `string` | `varchar` | `PK` | `unique` | LAN_CODE |
| `lan_desc` | `string` | `varchar` |  |  | LAN_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

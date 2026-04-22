# r5webservicemapping

eam_R5WEBSERVICEMAPPING

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5webservicemapping` |
| **Materialization** | `incremental` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wsg_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WSG_LASTSAVED |
| `wsg_tab` | `string` | `varchar` |  |  | WSG_TAB |
| `wsg_wsname` | `string` | `varchar` |  |  | WSG_WSNAME |
| `wsg_action` | `string` | `varchar` |  |  | WSG_ACTION |
| `wsg_packagename` | `string` | `varchar` |  |  | WSG_PACKAGENAME |
| `wsg_orgxpath` | `string` | `varchar` |  |  | WSG_ORGXPATH |
| `wsg_updatecount` | `float` | `float` |  |  | WSG_UPDATECOUNT |
| `wsg_function` | `string` | `varchar` |  |  | WSG_FUNCTION |
| `wsg_interfacecode` | `string` | `varchar` |  |  | WSG_INTERFACECODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

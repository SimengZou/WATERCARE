# r5udfscreens

eam_R5UDFSCREENS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5udfscreens` |
| **Materialization** | `incremental` |
| **Primary keys** | `usc_screenname` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `usc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | USC_LASTSAVED |
| `usc_tablename` | `string` | `varchar` |  |  | USC_TABLENAME |
| `usc_istab` | `string` | `varchar` |  |  | USC_ISTAB |
| `usc_parentscreen` | `string` | `varchar` |  |  | USC_PARENTSCREEN |
| `usc_generated` | `string` | `varchar` |  |  | USC_GENERATED |
| `usc_notused` | `string` | `varchar` |  |  | USC_NOTUSED |
| `usc_created` | `timestamp` | `timestamp_ntz` |  |  | USC_CREATED |
| `usc_updated` | `timestamp` | `timestamp_ntz` |  |  | USC_UPDATED |
| `usc_createdby` | `string` | `varchar` |  |  | USC_CREATEDBY |
| `usc_updatedby` | `string` | `varchar` |  |  | USC_UPDATEDBY |
| `usc_updatecount` | `float` | `float` |  |  | USC_UPDATECOUNT |
| `usc_entity` | `string` | `varchar` |  |  | USC_ENTITY |
| `usc_allowcomments` | `string` | `varchar` |  |  | USC_ALLOWCOMMENTS |
| `usc_allowdocuments` | `string` | `varchar` |  |  | USC_ALLOWDOCUMENTS |
| `usc_typeentity` | `string` | `varchar` |  |  | USC_TYPEENTITY |
| `usc_statusentity` | `string` | `varchar` |  |  | USC_STATUSENTITY |
| `usc_class` | `string` | `varchar` |  |  | USC_CLASS |
| `usc_orgsecurity` | `string` | `varchar` |  |  | USC_ORGSECURITY |
| `usc_autogeneratekey` | `string` | `varchar` |  |  | USC_AUTOGENERATEKEY |
| `usc_screenname` | `string` | `varchar` | `PK` | `unique` | USC_SCREENNAME |
| `usc_desc` | `string` | `varchar` |  |  | USC_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

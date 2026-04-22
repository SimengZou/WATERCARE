# r5mailattribs

eam_R5MAILATTRIBS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mailattribs` |
| **Materialization** | `incremental` |
| **Primary keys** | `maa_pk` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `maa_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MAA_LASTSAVED |
| `maa_enteredby` | `string` | `varchar` |  |  | MAA_ENTEREDBY |
| `maa_comment` | `string` | `varchar` |  |  | MAA_COMMENT |
| `maa_insert` | `string` | `varchar` |  |  | MAA_INSERT |
| `maa_update` | `string` | `varchar` |  |  | MAA_UPDATE |
| `maa_delete` | `string` | `varchar` |  |  | MAA_DELETE |
| `maa_oldstatus` | `string` | `varchar` |  |  | MAA_OLDSTATUS |
| `maa_newstatus` | `string` | `varchar` |  |  | MAA_NEWSTATUS |
| `maa_workflow` | `string` | `varchar` |  |  | MAA_WORKFLOW |
| `maa_updatecount` | `float` | `float` |  |  | MAA_UPDATECOUNT |
| `maa_pk` | `string` | `varchar` | `PK` | `unique` | MAA_PK |
| `maa_active` | `string` | `varchar` |  |  | MAA_ACTIVE |
| `maa_includeurl` | `string` | `varchar` |  |  | MAA_INCLUDEURL |
| `maa_includeattachment` | `string` | `varchar` |  |  | MAA_INCLUDEATTACHMENT |
| `maa_table` | `string` | `varchar` |  |  | MAA_TABLE |
| `maa_template` | `string` | `varchar` |  |  | MAA_TEMPLATE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

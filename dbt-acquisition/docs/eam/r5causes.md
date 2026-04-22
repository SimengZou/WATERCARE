# r5causes

eam_R5CAUSES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5causes` |
| **Materialization** | `incremental` |
| **Primary keys** | `cau_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cau_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CAU_LASTSAVED |
| `cau_desc` | `string` | `varchar` |  |  | CAU_DESC |
| `cau_gen` | `string` | `varchar` |  |  | CAU_GEN |
| `cau_updatecount` | `float` | `float` |  |  | CAU_UPDATECOUNT |
| `cau_created` | `timestamp` | `timestamp_ntz` |  |  | CAU_CREATED |
| `cau_partfailure` | `string` | `varchar` |  |  | CAU_PARTFAILURE |
| `cau_notused` | `string` | `varchar` |  |  | CAU_NOTUSED |
| `cau_enableworkorders` | `string` | `varchar` |  |  | CAU_ENABLEWORKORDERS |
| `cau_group` | `string` | `varchar` |  |  | CAU_GROUP |
| `cau_code` | `string` | `varchar` | `PK` | `unique` | CAU_CODE |
| `cau_updated` | `timestamp` | `timestamp_ntz` |  |  | CAU_UPDATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

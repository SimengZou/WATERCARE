# r5install

eam_R5INSTALL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5install` |
| **Materialization** | `incremental` |
| **Primary keys** | `ins_code` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ins_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | INS_LASTSAVED |
| `ins_desc` | `string` | `varchar` |  |  | INS_DESC |
| `ins_comment` | `string` | `varchar` |  |  | INS_COMMENT |
| `ins_fixed` | `string` | `varchar` |  |  | INS_FIXED |
| `ins_updatecount` | `float` | `float` |  |  | INS_UPDATECOUNT |
| `ins_extended` | `string` | `varchar` |  |  | INS_EXTENDED |
| `ins_edcomment` | `string` | `varchar` |  |  | INS_EDCOMMENT |
| `ins_code` | `string` | `varchar` | `PK` | `unique` | INS_CODE |
| `ins_module` | `string` | `varchar` |  |  | INS_MODULE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

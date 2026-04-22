# r5currencies

eam_R5CURRENCIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5currencies` |
| **Materialization** | `incremental` |
| **Primary keys** | `cur_code` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cur_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CUR_LASTSAVED |
| `cur_desc` | `string` | `varchar` |  |  | CUR_DESC |
| `cur_class` | `string` | `varchar` |  |  | CUR_CLASS |
| `cur_sourcesystem` | `string` | `varchar` |  |  | CUR_SOURCESYSTEM |
| `cur_sourcecode` | `string` | `varchar` |  |  | CUR_SOURCECODE |
| `cur_interface` | `timestamp` | `timestamp_ntz` |  |  | CUR_INTERFACE |
| `cur_dual` | `float` | `float` |  |  | CUR_DUAL |
| `cur_class_org` | `string` | `varchar` |  |  | CUR_CLASS_ORG |
| `cur_updatecount` | `float` | `float` |  |  | CUR_UPDATECOUNT |
| `cur_created` | `timestamp` | `timestamp_ntz` |  |  | CUR_CREATED |
| `cur_updated` | `timestamp` | `timestamp_ntz` |  |  | CUR_UPDATED |
| `cur_code` | `string` | `varchar` | `PK` | `unique` | CUR_CODE |
| `cur_notused` | `string` | `varchar` |  |  | CUR_NOTUSED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

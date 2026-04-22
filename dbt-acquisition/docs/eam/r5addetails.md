# r5addetails

eam_R5ADDETAILS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5addetails` |
| **Materialization** | `incremental` |
| **Primary keys** | `add_code`, `add_rentity`, `add_type`, `add_lang`, `add_line` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `add_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ADD_LASTSAVED |
| `add_type` | `string` | `varchar` | `PK` |  | ADD_TYPE |
| `add_rtype` | `string` | `varchar` |  |  | ADD_RTYPE |
| `add_code` | `string` | `varchar` | `PK` |  | ADD_CODE |
| `add_lang` | `string` | `varchar` | `PK` |  | ADD_LANG |
| `add_line` | `float` | `float` | `PK` |  | ADD_LINE |
| `add_print` | `string` | `varchar` |  |  | ADD_PRINT |
| `add_text` | `string` | `varchar` |  |  | ADD_TEXT. Max length: 0 |
| `add_created` | `timestamp` | `timestamp_ntz` |  |  | ADD_CREATED |
| `add_user` | `string` | `varchar` |  |  | ADD_USER |
| `add_updated` | `timestamp` | `timestamp_ntz` |  |  | ADD_UPDATED |
| `add_upduser` | `string` | `varchar` |  |  | ADD_UPDUSER |
| `add_updatecount` | `float` | `float` |  |  | ADD_UPDATECOUNT |
| `add_entity` | `string` | `varchar` |  |  | ADD_ENTITY |
| `add_rentity` | `string` | `varchar` | `PK` |  | ADD_RENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

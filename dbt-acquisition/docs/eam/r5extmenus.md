# r5extmenus

eam_R5EXTMENUS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5extmenus` |
| **Materialization** | `incremental` |
| **Primary keys** | `emn_code` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `emn_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EMN_LASTSAVED |
| `emn_sequence` | `float` | `float` |  |  | EMN_SEQUENCE |
| `emn_group` | `string` | `varchar` |  |  | EMN_GROUP |
| `emn_function` | `string` | `varchar` |  |  | EMN_FUNCTION |
| `emn_parent` | `string` | `varchar` |  |  | EMN_PARENT |
| `emn_type` | `string` | `varchar` |  |  | EMN_TYPE |
| `emn_duxmobile` | `string` | `varchar` |  |  | EMN_DUXMOBILE |
| `emn_meflag` | `string` | `varchar` |  |  | EMN_MEFLAG |
| `emn_hide` | `string` | `varchar` |  |  | EMN_HIDE |
| `emn_mobile` | `string` | `varchar` |  |  | EMN_MOBILE |
| `emn_icon` | `string` | `varchar` |  |  | EMN_ICON |
| `emn_transit` | `string` | `varchar` |  |  | EMN_TRANSIT |
| `emn_code` | `string` | `varchar` | `PK` | `unique` | EMN_CODE |
| `emn_updatecount` | `float` | `float` |  |  | EMN_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

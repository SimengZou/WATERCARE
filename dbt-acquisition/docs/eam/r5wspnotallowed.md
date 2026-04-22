# r5wspnotallowed

eam_R5WSPNOTALLOWED

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wspnotallowed` |
| **Materialization** | `incremental` |
| **Primary keys** | `wpa_function`, `wpa_tab`, `wpa_action`, `wpa_alltabs` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wpa_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WPA_LASTSAVED |
| `wpa_tab` | `string` | `varchar` | `PK` |  | WPA_TAB |
| `wpa_alltabs` | `string` | `varchar` | `PK` |  | WPA_ALLTABS |
| `wpa_function` | `string` | `varchar` | `PK` |  | WPA_FUNCTION |
| `wpa_action` | `string` | `varchar` | `PK` |  | WPA_ACTION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5tabnoperm

eam_R5TABNOPERM

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5tabnoperm` |
| **Materialization** | `incremental` |
| **Primary keys** | `tpn_function`, `tpn_tab` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tpn_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TPN_LASTSAVED |
| `tpn_tab` | `string` | `varchar` | `PK` |  | TPN_TAB |
| `tpn_noselect` | `string` | `varchar` |  |  | TPN_NOSELECT |
| `tpn_nodelete` | `string` | `varchar` |  |  | TPN_NODELETE |
| `tpn_noupdate` | `string` | `varchar` |  |  | TPN_NOUPDATE |
| `tpn_updatecount` | `float` | `float` |  |  | TPN_UPDATECOUNT |
| `tpn_function` | `string` | `varchar` | `PK` |  | TPN_FUNCTION |
| `tpn_noinsert` | `string` | `varchar` |  |  | TPN_NOINSERT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

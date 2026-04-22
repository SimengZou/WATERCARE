# r5homegroups

eam_R5HOMEGROUPS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5homegroups` |
| **Materialization** | `incremental` |
| **Primary keys** | `hmg_homcode`, `hmg_homtype`, `hmg_group` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `hmg_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | HMG_LASTSAVED |
| `hmg_homtype` | `string` | `varchar` | `PK` |  | HMG_HOMTYPE |
| `hmg_group` | `string` | `varchar` | `PK` |  | HMG_GROUP |
| `hmg_seq` | `float` | `float` |  |  | HMG_SEQ |
| `hmg_autofresh` | `string` | `varchar` |  |  | HMG_AUTOFRESH |
| `hmg_tab` | `string` | `varchar` |  |  | HMG_TAB |
| `hmg_homcode` | `string` | `varchar` | `PK` |  | HMG_HOMCODE |
| `hmg_updatecount` | `float` | `float` |  |  | HMG_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

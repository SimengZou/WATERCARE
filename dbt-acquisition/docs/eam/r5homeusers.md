# r5homeusers

eam_R5HOMEUSERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5homeusers` |
| **Materialization** | `incremental` |
| **Primary keys** | `hmu_homcode`, `hmu_homtype`, `hmu_user`, `hmu_seq` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `hmu_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | HMU_LASTSAVED |
| `hmu_homtype` | `string` | `varchar` | `PK` |  | HMU_HOMTYPE |
| `hmu_user` | `string` | `varchar` | `PK` |  | HMU_USER |
| `hmu_autofresh` | `string` | `varchar` |  |  | HMU_AUTOFRESH |
| `hmu_updatecount` | `float` | `float` |  |  | HMU_UPDATECOUNT |
| `hmu_tab` | `string` | `varchar` |  |  | HMU_TAB |
| `hmu_homcode` | `string` | `varchar` | `PK` |  | HMU_HOMCODE |
| `hmu_seq` | `float` | `float` | `PK` |  | HMU_SEQ |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

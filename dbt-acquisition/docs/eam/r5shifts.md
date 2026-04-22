# r5shifts

eam_R5SHIFTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5shifts` |
| **Materialization** | `incremental` |
| **Primary keys** | `shf_code` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `shf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SHF_LASTSAVED |
| `shf_desc` | `string` | `varchar` |  |  | SHF_DESC |
| `shf_class` | `string` | `varchar` |  |  | SHF_CLASS |
| `shf_length` | `float` | `float` |  |  | SHF_LENGTH |
| `shf_org` | `string` | `varchar` |  |  | SHF_ORG |
| `shf_class_org` | `string` | `varchar` |  |  | SHF_CLASS_ORG |
| `shf_updatecount` | `float` | `float` |  |  | SHF_UPDATECOUNT |
| `shf_updated` | `timestamp` | `timestamp_ntz` |  |  | SHF_UPDATED |
| `shf_code` | `string` | `varchar` | `PK` | `unique` | SHF_CODE |
| `shf_start` | `timestamp` | `timestamp_ntz` |  |  | SHF_START |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

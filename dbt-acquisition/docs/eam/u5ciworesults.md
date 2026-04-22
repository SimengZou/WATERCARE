# u5ciworesults

eam_U5CIWORESULTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5ciworesults` |
| **Materialization** | `incremental` |
| **Primary keys** | `cir_sequence` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `cir_workordernum` | `string` | `varchar` |  |  | CIR_WORKORDERNUM |
| `cir_type` | `string` | `varchar` |  |  | CIR_TYPE |
| `cir_code` | `string` | `varchar` |  |  | CIR_CODE |
| `cir_detail` | `string` | `varchar` |  |  | CIR_DETAIL |
| `cir_obsvalue` | `string` | `varchar` |  |  | CIR_OBSVALUE |
| `cir_obsdate` | `timestamp` | `timestamp_ntz` |  |  | CIR_OBSDATE |
| `cir_obsdescription` | `string` | `varchar` |  |  | CIR_OBSDESCRIPTION |
| `cir_obsuom` | `string` | `varchar` |  |  | CIR_OBSUOM |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `cir_sequence` | `float` | `float` | `PK` | `unique` | CIR_SEQUENCE |
| `cir_transid` | `string` | `varchar` |  |  | CIR_TRANSID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

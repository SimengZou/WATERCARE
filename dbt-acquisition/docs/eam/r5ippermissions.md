# r5ippermissions

eam_R5IPPERMISSIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5ippermissions` |
| **Materialization** | `incremental` |
| **Primary keys** | `ipp_code` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ipp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | IPP_LASTSAVED |
| `ipp_mnemonic` | `string` | `varchar` |  |  | IPP_MNEMONIC |
| `ipp_updatecount` | `float` | `float` |  |  | IPP_UPDATECOUNT |
| `ipp_code` | `float` | `float` | `PK` | `unique` | IPP_CODE |
| `ipp_function` | `float` | `float` |  |  | IPP_FUNCTION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

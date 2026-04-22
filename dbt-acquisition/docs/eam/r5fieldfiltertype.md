# r5fieldfiltertype

eam_R5FIELDFILTERTYPE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5fieldfiltertype` |
| **Materialization** | `incremental` |
| **Primary keys** | `fft_function`, `fft_type`, `fft_rtype` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `fft_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FFT_LASTSAVED |
| `fft_type` | `string` | `varchar` | `PK` |  | FFT_TYPE |
| `fft_defaultscreentype` | `string` | `varchar` |  |  | FFT_DEFAULTSCREENTYPE |
| `fft_updatecount` | `float` | `float` |  |  | FFT_UPDATECOUNT |
| `fft_function` | `string` | `varchar` | `PK` |  | FFT_FUNCTION |
| `fft_rtype` | `string` | `varchar` | `PK` |  | FFT_RTYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

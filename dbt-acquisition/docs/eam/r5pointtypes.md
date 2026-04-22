# r5pointtypes

eam_R5POINTTYPES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pointtypes` |
| **Materialization** | `incremental` |
| **Primary keys** | `ptp_code` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ptp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PTP_LASTSAVED |
| `ptp_desc` | `string` | `varchar` |  |  | PTP_DESC |
| `ptp_class` | `string` | `varchar` |  |  | PTP_CLASS |
| `ptp_updatecount` | `float` | `float` |  |  | PTP_UPDATECOUNT |
| `ptp_created` | `timestamp` | `timestamp_ntz` |  |  | PTP_CREATED |
| `ptp_updated` | `timestamp` | `timestamp_ntz` |  |  | PTP_UPDATED |
| `ptp_code` | `string` | `varchar` | `PK` | `unique` | PTP_CODE |
| `ptp_class_org` | `string` | `varchar` |  |  | PTP_CLASS_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

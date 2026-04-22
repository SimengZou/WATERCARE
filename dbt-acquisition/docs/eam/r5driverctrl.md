# r5driverctrl

eam_R5DRIVERCTRL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5driverctrl` |
| **Materialization** | `incremental` |
| **Primary keys** | `drv_code`, `drv_queue` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `drv_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DRV_LASTSAVED |
| `drv_queue` | `string` | `varchar` | `PK` |  | DRV_QUEUE |
| `drv_action` | `string` | `varchar` |  |  | DRV_ACTION |
| `drv_nextrun` | `timestamp` | `timestamp_ntz` |  |  | DRV_NEXTRUN |
| `drv_frequency` | `float` | `float` |  |  | DRV_FREQUENCY |
| `drv_code` | `string` | `varchar` | `PK` |  | DRV_CODE |
| `drv_lastran` | `timestamp` | `timestamp_ntz` |  |  | DRV_LASTRAN |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

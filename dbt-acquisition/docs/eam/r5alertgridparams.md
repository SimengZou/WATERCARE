# r5alertgridparams

eam_R5ALERTGRIDPARAMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alertgridparams` |
| **Materialization** | `incremental` |
| **Primary keys** | `agp_alert`, `agp_param` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `agp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AGP_LASTSAVED |
| `agp_param` | `string` | `varchar` | `PK` |  | AGP_PARAM |
| `agp_value` | `string` | `varchar` |  |  | AGP_VALUE |
| `agp_dvalue` | `timestamp` | `timestamp_ntz` |  |  | AGP_DVALUE |
| `agp_updatecount` | `float` | `float` |  |  | AGP_UPDATECOUNT |
| `agp_alert` | `string` | `varchar` | `PK` |  | AGP_ALERT |
| `agp_nvalue` | `float` | `float` |  |  | AGP_NVALUE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

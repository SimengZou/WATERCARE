# r5docentities

eam_R5DOCENTITIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5docentities` |
| **Materialization** | `incremental` |
| **Primary keys** | `dae_document`, `dae_code`, `dae_rentity` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dae_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DAE_LASTSAVED |
| `dae_entity` | `string` | `varchar` |  |  | DAE_ENTITY |
| `dae_rentity` | `string` | `varchar` | `PK` |  | DAE_RENTITY |
| `dae_type` | `string` | `varchar` |  |  | DAE_TYPE |
| `dae_rtype` | `string` | `varchar` |  |  | DAE_RTYPE |
| `dae_code` | `string` | `varchar` | `PK` |  | DAE_CODE |
| `dae_idmcopy` | `string` | `varchar` |  |  | DAE_IDMCOPY |
| `dae_printonwo` | `string` | `varchar` |  |  | DAE_PRINTONWO |
| `dae_copytopo` | `string` | `varchar` |  |  | DAE_COPYTOPO |
| `dae_printonpo` | `string` | `varchar` |  |  | DAE_PRINTONPO |
| `dae_updatecount` | `float` | `float` |  |  | DAE_UPDATECOUNT |
| `dae_createcopytowo` | `string` | `varchar` |  |  | DAE_CREATECOPYTOWO |
| `dae_document` | `string` | `varchar` | `PK` |  | DAE_DOCUMENT |
| `dae_copytowo` | `string` | `varchar` |  |  | DAE_COPYTOWO |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

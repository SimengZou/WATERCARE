# r5properties

eam_R5PROPERTIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5properties` |
| **Materialization** | `incremental` |
| **Primary keys** | `pro_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pro_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PRO_LASTSAVED |
| `pro_type` | `string` | `varchar` |  |  | PRO_TYPE |
| `pro_text` | `string` | `varchar` |  |  | PRO_TEXT |
| `pro_min` | `string` | `varchar` |  |  | PRO_MIN |
| `pro_max` | `string` | `varchar` |  |  | PRO_MAX |
| `pro_updatecount` | `float` | `float` |  |  | PRO_UPDATECOUNT |
| `pro_created` | `timestamp` | `timestamp_ntz` |  |  | PRO_CREATED |
| `pro_updated` | `timestamp` | `timestamp_ntz` |  |  | PRO_UPDATED |
| `pro_includeingrids` | `string` | `varchar` |  |  | PRO_INCLUDEINGRIDS |
| `pro_code` | `string` | `varchar` | `PK` | `unique` | PRO_CODE |
| `pro_rentity` | `string` | `varchar` |  |  | PRO_RENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

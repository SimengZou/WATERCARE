# r5ucodes

eam_R5UCODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5ucodes` |
| **Materialization** | `incremental` |
| **Primary keys** | `uco_entity`, `uco_code` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `uco_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UCO_LASTSAVED |
| `uco_image` | `string` | `varchar` |  |  | UCO_IMAGE |
| `uco_code` | `string` | `varchar` | `PK` |  | UCO_CODE |
| `uco_rcode` | `string` | `varchar` |  |  | UCO_RCODE |
| `uco_system` | `string` | `varchar` |  |  | UCO_SYSTEM |
| `uco_desc` | `string` | `varchar` |  |  | UCO_DESC |
| `uco_updatecount` | `float` | `float` |  |  | UCO_UPDATECOUNT |
| `uco_created` | `timestamp` | `timestamp_ntz` |  |  | UCO_CREATED |
| `uco_updated` | `timestamp` | `timestamp_ntz` |  |  | UCO_UPDATED |
| `uco_textcode` | `string` | `varchar` |  |  | UCO_TEXTCODE |
| `uco_notused` | `string` | `varchar` |  |  | UCO_NOTUSED |
| `uco_icon` | `string` | `varchar` |  |  | UCO_ICON |
| `uco_iconpath` | `string` | `varchar` |  |  | UCO_ICONPATH |
| `uco_weight` | `float` | `float` |  |  | UCO_WEIGHT |
| `uco_color` | `string` | `varchar` |  |  | UCO_COLOR |
| `uco_category` | `string` | `varchar` |  |  | UCO_CATEGORY |
| `uco_gisdispatchranking` | `float` | `float` |  |  | UCO_GISDISPATCHRANKING |
| `uco_cuadjustment` | `float` | `float` |  |  | UCO_CUADJUSTMENT |
| `uco_entity` | `string` | `varchar` | `PK` |  | UCO_ENTITY |
| `uco_rentity` | `string` | `varchar` |  |  | UCO_RENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

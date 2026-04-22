# r5routobjects

eam_R5ROUTOBJECTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5routobjects` |
| **Materialization** | `incremental` |
| **Primary keys** | `rob_route`, `rob_revision`, `rob_line` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rob_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ROB_LASTSAVED |
| `rob_obtype` | `string` | `varchar` |  |  | ROB_OBTYPE |
| `rob_obrtype` | `string` | `varchar` |  |  | ROB_OBRTYPE |
| `rob_object` | `string` | `varchar` |  |  | ROB_OBJECT |
| `rob_revision` | `float` | `float` | `PK` |  | ROB_REVISION |
| `rob_object_org` | `string` | `varchar` |  |  | ROB_OBJECT_ORG |
| `rob_updatecount` | `float` | `float` |  |  | ROB_UPDATECOUNT |
| `rob_frompoint` | `float` | `float` |  |  | ROB_FROMPOINT |
| `rob_fromrefdesc` | `string` | `varchar` |  |  | ROB_FROMREFDESC |
| `rob_fromgeoref` | `string` | `varchar` |  |  | ROB_FROMGEOREF |
| `rob_topoint` | `float` | `float` |  |  | ROB_TOPOINT |
| `rob_torefdesc` | `string` | `varchar` |  |  | ROB_TOREFDESC |
| `rob_togeoref` | `string` | `varchar` |  |  | ROB_TOGEOREF |
| `rob_from_reference` | `float` | `float` |  |  | ROB_FROM_REFERENCE |
| `rob_from_offset` | `float` | `float` |  |  | ROB_FROM_OFFSET |
| `rob_from_offset_direction` | `string` | `varchar` |  |  | ROB_FROM_OFFSET_DIRECTION |
| `rob_to_reference` | `float` | `float` |  |  | ROB_TO_REFERENCE |
| `rob_to_offset` | `float` | `float` |  |  | ROB_TO_OFFSET |
| `rob_to_offset_direction` | `string` | `varchar` |  |  | ROB_TO_OFFSET_DIRECTION |
| `rob_route` | `string` | `varchar` | `PK` |  | ROB_ROUTE |
| `rob_line` | `float` | `float` | `PK` |  | ROB_LINE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

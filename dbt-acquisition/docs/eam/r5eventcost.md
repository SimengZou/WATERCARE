# r5eventcost

eam_R5EVENTCOST

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5eventcost` |
| **Materialization** | `incremental` |
| **Primary keys** | `evo_event` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `evo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EVO_LASTSAVED |
| `evo_directmaterial` | `float` | `float` |  |  | EVO_DIRECTMATERIAL |
| `evo_costcalculated` | `string` | `varchar` |  |  | EVO_COSTCALCULATED |
| `evo_labor` | `float` | `float` |  |  | EVO_LABOR |
| `evo_material` | `float` | `float` |  |  | EVO_MATERIAL |
| `evo_tool` | `float` | `float` |  |  | EVO_TOOL |
| `evo_total` | `float` | `float` |  |  | EVO_TOTAL |
| `evo_hours` | `float` | `float` |  |  | EVO_HOURS |
| `evo_updated` | `timestamp` | `timestamp_ntz` |  |  | EVO_UPDATED |
| `evo_ownlabor` | `float` | `float` |  |  | EVO_OWNLABOR |
| `evo_hiredlabor` | `float` | `float` |  |  | EVO_HIREDLABOR |
| `evo_serviceslabor` | `float` | `float` |  |  | EVO_SERVICESLABOR |
| `evo_stockmaterial` | `float` | `float` |  |  | EVO_STOCKMATERIAL |
| `evo_event` | `string` | `varchar` | `PK` | `unique` | EVO_EVENT |
| `evo_recalccost` | `string` | `varchar` |  |  | EVO_RECALCCOST |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

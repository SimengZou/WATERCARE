# r5eventobjects

eam_R5EVENTOBJECTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5eventobjects` |
| **Materialization** | `incremental` |
| **Primary keys** | `eob_event`, `eob_object`, `eob_object_org` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `eob_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EOB_LASTSAVED |
| `eob_obtype` | `string` | `varchar` |  |  | EOB_OBTYPE |
| `eob_obrtype` | `string` | `varchar` |  |  | EOB_OBRTYPE |
| `eob_object` | `string` | `varchar` | `PK` |  | EOB_OBJECT |
| `eob_level` | `float` | `float` |  |  | EOB_LEVEL |
| `eob_rollup` | `string` | `varchar` |  |  | EOB_ROLLUP |
| `eob_object_org` | `string` | `varchar` | `PK` |  | EOB_OBJECT_ORG |
| `eob_updatecount` | `float` | `float` |  |  | EOB_UPDATECOUNT |
| `eob_frompoint` | `float` | `float` |  |  | EOB_FROMPOINT |
| `eob_topoint` | `float` | `float` |  |  | EOB_TOPOINT |
| `eob_weightpercentage` | `float` | `float` |  |  | EOB_WEIGHTPERCENTAGE |
| `eob_event` | `string` | `varchar` | `PK` |  | EOB_EVENT |
| `eob_downtime` | `string` | `varchar` |  |  | EOB_DOWNTIME |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

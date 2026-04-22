# r5firstact

eam_R5FIRSTACT

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5firstact` |
| **Materialization** | `incremental` |
| **Primary keys** | `act_event` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `act_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ACT_LASTSAVED |
| `act_complocation` | `string` | `varchar` |  |  | ACT_COMPLOCATION |
| `act_start` | `timestamp` | `timestamp_ntz` |  |  | ACT_START |
| `act_trade` | `string` | `varchar` |  |  | ACT_TRADE |
| `act_persons` | `float` | `float` |  |  | ACT_PERSONS |
| `act_duration` | `float` | `float` |  |  | ACT_DURATION |
| `act_est` | `float` | `float` |  |  | ACT_EST |
| `act_rem` | `float` | `float` |  |  | ACT_REM |
| `act_task` | `string` | `varchar` |  |  | ACT_TASK |
| `act_matlist` | `string` | `varchar` |  |  | ACT_MATLIST |
| `act_multipletrades` | `string` | `varchar` |  |  | ACT_MULTIPLETRADES |
| `act_rpc` | `string` | `varchar` |  |  | ACT_RPC |
| `act_wap` | `string` | `varchar` |  |  | ACT_WAP |
| `act_tpf` | `string` | `varchar` |  |  | ACT_TPF |
| `act_manufact` | `string` | `varchar` |  |  | ACT_MANUFACT |
| `act_syslevel` | `string` | `varchar` |  |  | ACT_SYSLEVEL |
| `act_asmlevel` | `string` | `varchar` |  |  | ACT_ASMLEVEL |
| `act_complevel` | `string` | `varchar` |  |  | ACT_COMPLEVEL |
| `act_event` | `string` | `varchar` | `PK` | `unique` | ACT_EVENT |
| `act_act` | `float` | `float` |  |  | ACT_ACT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

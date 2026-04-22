# r5actorl

eam_R5ACTORL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5actorl` |
| **Materialization** | `incremental` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `act_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ACT_LASTSAVED |
| `act_act` | `float` | `float` |  |  | ACT_ACT |
| `evt_org` | `string` | `varchar` |  |  | EVT_ORG |
| `act_order_org` | `string` | `varchar` |  |  | ACT_ORDER_ORG |
| `act_ordline` | `float` | `float` |  |  | ACT_ORDLINE |
| `act_event` | `string` | `varchar` |  |  | ACT_EVENT |
| `act_order` | `string` | `varchar` |  |  | ACT_ORDER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

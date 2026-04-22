# r5reliabilityrankinganswer

eam_R5RELIABILITYRANKINGANSWER

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reliabilityrankinganswer` |
| **Materialization** | `incremental` |
| **Primary keys** | `rrw_pk` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rrw_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RRW_LASTSAVED |
| `rrw_severity` | `string` | `varchar` |  |  | RRW_SEVERITY |
| `rrw_code` | `string` | `varchar` |  |  | RRW_CODE |
| `rrw_desc` | `string` | `varchar` |  |  | RRW_DESC |
| `rrw_value` | `float` | `float` |  |  | RRW_VALUE |
| `rrw_updatecount` | `float` | `float` |  |  | RRW_UPDATECOUNT |
| `rrw_yes` | `string` | `varchar` |  |  | RRW_YES |
| `rrw_no` | `string` | `varchar` |  |  | RRW_NO |
| `rrw_finding` | `string` | `varchar` |  |  | RRW_FINDING |
| `rrw_ok` | `string` | `varchar` |  |  | RRW_OK |
| `rrw_repairsneeded` | `string` | `varchar` |  |  | RRW_REPAIRSNEEDED |
| `rrw_resolution` | `string` | `varchar` |  |  | RRW_RESOLUTION |
| `rrw_good` | `string` | `varchar` |  |  | RRW_GOOD |
| `rrw_poor` | `string` | `varchar` |  |  | RRW_POOR |
| `rrw_adjusted` | `string` | `varchar` |  |  | RRW_ADJUSTED |
| `rrw_nonconformity` | `string` | `varchar` |  |  | RRW_NONCONFORMITY |
| `rrw_pk` | `string` | `varchar` | `PK` | `unique` | RRW_PK |
| `rrw_levelpk` | `string` | `varchar` |  |  | RRW_LEVELPK |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

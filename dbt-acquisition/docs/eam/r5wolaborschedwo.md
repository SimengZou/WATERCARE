# r5wolaborschedwo

eam_R5WOLABORSCHEDWO

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wolaborschedwo` |
| **Materialization** | `incremental` |
| **Primary keys** | `lsw_sessionid`, `lsw_event`, `lsw_act` |
| **Column count** | 33 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lsw_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LSW_LASTSAVED |
| `lsw_act` | `float` | `float` | `PK` |  | LSW_ACT |
| `lsw_select` | `string` | `varchar` |  |  | LSW_SELECT |
| `lsw_hrsrem` | `float` | `float` |  |  | LSW_HRSREM |
| `lsw_status` | `string` | `varchar` |  |  | LSW_STATUS |
| `lsw_newstatus` | `string` | `varchar` |  |  | LSW_NEWSTATUS |
| `lsw_pplrem` | `float` | `float` |  |  | LSW_PPLREM |
| `lsw_acthrsrem` | `float` | `float` |  |  | LSW_ACTHRSREM |
| `lsw_actpplreq` | `float` | `float` |  |  | LSW_ACTPPLREQ |
| `lsw_prevschedhrs` | `float` | `float` |  |  | LSW_PREVSCHEDHRS |
| `lsw_prevschedppl` | `float` | `float` |  |  | LSW_PREVSCHEDPPL |
| `lsw_totalschedhrs` | `float` | `float` |  |  | LSW_TOTALSCHEDHRS |
| `lsw_lastscheddate` | `timestamp` | `timestamp_ntz` |  |  | LSW_LASTSCHEDDATE |
| `lsw_matavail` | `timestamp` | `timestamp_ntz` |  |  | LSW_MATAVAIL |
| `lsw_related` | `string` | `varchar` |  |  | LSW_RELATED |
| `lsw_ignore` | `string` | `varchar` |  |  | LSW_IGNORE |
| `lsw_esignuser` | `string` | `varchar` |  |  | LSW_ESIGNUSER |
| `lsw_esigntype` | `string` | `varchar` |  |  | LSW_ESIGNTYPE |
| `lsw_esigndate` | `timestamp` | `timestamp_ntz` |  |  | LSW_ESIGNDATE |
| `lsw_esigncertifynum` | `string` | `varchar` |  |  | LSW_ESIGNCERTIFYNUM |
| `lsw_esigncertifytype` | `string` | `varchar` |  |  | LSW_ESIGNCERTIFYTYPE |
| `lsw_actcompleted` | `string` | `varchar` |  |  | LSW_ACTCOMPLETED |
| `lsw_updatecount` | `float` | `float` |  |  | LSW_UPDATECOUNT |
| `lsw_contnextshift` | `string` | `varchar` |  |  | LSW_CONTNEXTSHIFT |
| `lsw_dueperc` | `float` | `float` |  |  | LSW_DUEPERC |
| `lsw_sessionid` | `float` | `float` | `PK` |  | LSW_SESSIONID |
| `lsw_event` | `string` | `varchar` | `PK` |  | LSW_EVENT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

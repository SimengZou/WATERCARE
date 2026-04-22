# r5foreignentity

eam_R5FOREIGNENTITY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5foreignentity` |
| **Materialization** | `incremental` |
| **Primary keys** | `fen_userfunction`, `fen_systemfunction`, `fen_foreignuserfunction`, `fen_foreignsystemfunction`, `fen_elementid`, `fen_foreignelementid`, `fen_rentity` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `fen_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FEN_LASTSAVED |
| `fen_systemfunction` | `string` | `varchar` | `PK` |  | FEN_SYSTEMFUNCTION |
| `fen_foreignuserfunction` | `string` | `varchar` | `PK` |  | FEN_FOREIGNUSERFUNCTION |
| `fen_foreignsystemfunction` | `string` | `varchar` | `PK` |  | FEN_FOREIGNSYSTEMFUNCTION |
| `fen_elementid` | `string` | `varchar` | `PK` |  | FEN_ELEMENTID |
| `fen_orgfield` | `string` | `varchar` |  |  | FEN_ORGFIELD |
| `fen_rentity` | `string` | `varchar` | `PK` |  | FEN_RENTITY |
| `fen_updatecount` | `float` | `float` |  |  | FEN_UPDATECOUNT |
| `fen_allowdrillback` | `string` | `varchar` |  |  | FEN_ALLOWDRILLBACK |
| `fen_userfunction` | `string` | `varchar` | `PK` |  | FEN_USERFUNCTION |
| `fen_foreignelementid` | `string` | `varchar` | `PK` |  | FEN_FOREIGNELEMENTID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5funcusers

eam_R5FUNCUSERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5funcusers` |
| **Materialization** | `incremental` |
| **Primary keys** | `fus_user`, `fus_function` |
| **Column count** | 32 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `fus_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FUS_LASTSAVED |
| `fus_shortn` | `string` | `varchar` |  |  | FUS_SHORTN |
| `fus_printer` | `string` | `varchar` |  |  | FUS_PRINTER |
| `fus_butfun1` | `string` | `varchar` |  |  | FUS_BUTFUN1 |
| `fus_butfun2` | `string` | `varchar` |  |  | FUS_BUTFUN2 |
| `fus_butfun3` | `string` | `varchar` |  |  | FUS_BUTFUN3 |
| `fus_butfun4` | `string` | `varchar` |  |  | FUS_BUTFUN4 |
| `fus_butfun5` | `string` | `varchar` |  |  | FUS_BUTFUN5 |
| `fus_butfun6` | `string` | `varchar` |  |  | FUS_BUTFUN6 |
| `fus_butname1` | `string` | `varchar` |  |  | FUS_BUTNAME1 |
| `fus_butname2` | `string` | `varchar` |  |  | FUS_BUTNAME2 |
| `fus_butname3` | `string` | `varchar` |  |  | FUS_BUTNAME3 |
| `fus_butname4` | `string` | `varchar` |  |  | FUS_BUTNAME4 |
| `fus_butname5` | `string` | `varchar` |  |  | FUS_BUTNAME5 |
| `fus_butname6` | `string` | `varchar` |  |  | FUS_BUTNAME6 |
| `fus_buticon1` | `string` | `varchar` |  |  | FUS_BUTICON1 |
| `fus_buticon2` | `string` | `varchar` |  |  | FUS_BUTICON2 |
| `fus_buticon3` | `string` | `varchar` |  |  | FUS_BUTICON3 |
| `fus_buticon4` | `string` | `varchar` |  |  | FUS_BUTICON4 |
| `fus_buticon5` | `string` | `varchar` |  |  | FUS_BUTICON5 |
| `fus_buticon6` | `string` | `varchar` |  |  | FUS_BUTICON6 |
| `fus_notes` | `string` | `varchar` |  |  | FUS_NOTES |
| `fus_filter` | `string` | `varchar` |  |  | FUS_FILTER |
| `fus_updatecount` | `float` | `float` |  |  | FUS_UPDATECOUNT |
| `fus_function` | `string` | `varchar` | `PK` |  | FUS_FUNCTION |
| `fus_user` | `string` | `varchar` | `PK` |  | FUS_USER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5trackingdata

eam_R5TRACKINGDATA

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5trackingdata` |
| **Materialization** | `incremental` |
| **Primary keys** | `tkd_transid` |
| **Column count** | 67 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tkd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TKD_LASTSAVED |
| `tkd_sourcesystem` | `string` | `varchar` |  |  | TKD_SOURCESYSTEM |
| `tkd_sourcecode` | `string` | `varchar` |  |  | TKD_SOURCECODE |
| `tkd_trans` | `string` | `varchar` |  |  | TKD_TRANS |
| `tkd_trackdate` | `timestamp` | `timestamp_ntz` |  |  | TKD_TRACKDATE |
| `tkd_promptdata1` | `string` | `varchar` |  |  | TKD_PROMPTDATA1 |
| `tkd_promptdata2` | `string` | `varchar` |  |  | TKD_PROMPTDATA2 |
| `tkd_promptdata3` | `string` | `varchar` |  |  | TKD_PROMPTDATA3 |
| `tkd_promptdata4` | `string` | `varchar` |  |  | TKD_PROMPTDATA4 |
| `tkd_promptdata5` | `string` | `varchar` |  |  | TKD_PROMPTDATA5 |
| `tkd_promptdata6` | `string` | `varchar` |  |  | TKD_PROMPTDATA6 |
| `tkd_promptdata7` | `string` | `varchar` |  |  | TKD_PROMPTDATA7 |
| `tkd_promptdata8` | `string` | `varchar` |  |  | TKD_PROMPTDATA8 |
| `tkd_promptdata9` | `string` | `varchar` |  |  | TKD_PROMPTDATA9 |
| `tkd_promptdata10` | `string` | `varchar` |  |  | TKD_PROMPTDATA10 |
| `tkd_promptdata11` | `string` | `varchar` |  |  | TKD_PROMPTDATA11 |
| `tkd_promptdata12` | `string` | `varchar` |  |  | TKD_PROMPTDATA12 |
| `tkd_promptdata13` | `string` | `varchar` |  |  | TKD_PROMPTDATA13 |
| `tkd_promptdata14` | `string` | `varchar` |  |  | TKD_PROMPTDATA14 |
| `tkd_promptdata15` | `string` | `varchar` |  |  | TKD_PROMPTDATA15 |
| `tkd_promptdata16` | `string` | `varchar` |  |  | TKD_PROMPTDATA16 |
| `tkd_promptdata17` | `string` | `varchar` |  |  | TKD_PROMPTDATA17 |
| `tkd_promptdata18` | `string` | `varchar` |  |  | TKD_PROMPTDATA18 |
| `tkd_promptdata19` | `string` | `varchar` |  |  | TKD_PROMPTDATA19 |
| `tkd_promptdata20` | `string` | `varchar` |  |  | TKD_PROMPTDATA20 |
| `tkd_promptdata21` | `string` | `varchar` |  |  | TKD_PROMPTDATA21 |
| `tkd_promptdata22` | `string` | `varchar` |  |  | TKD_PROMPTDATA22 |
| `tkd_promptdata23` | `string` | `varchar` |  |  | TKD_PROMPTDATA23 |
| `tkd_promptdata24` | `string` | `varchar` |  |  | TKD_PROMPTDATA24 |
| `tkd_promptdata25` | `string` | `varchar` |  |  | TKD_PROMPTDATA25 |
| `tkd_promptdata26` | `string` | `varchar` |  |  | TKD_PROMPTDATA26 |
| `tkd_promptdata27` | `string` | `varchar` |  |  | TKD_PROMPTDATA27 |
| `tkd_promptdata28` | `string` | `varchar` |  |  | TKD_PROMPTDATA28 |
| `tkd_promptdata29` | `string` | `varchar` |  |  | TKD_PROMPTDATA29 |
| `tkd_promptdata30` | `string` | `varchar` |  |  | TKD_PROMPTDATA30 |
| `tkd_promptdata31` | `string` | `varchar` |  |  | TKD_PROMPTDATA31 |
| `tkd_promptdata32` | `string` | `varchar` |  |  | TKD_PROMPTDATA32 |
| `tkd_promptdata33` | `string` | `varchar` |  |  | TKD_PROMPTDATA33 |
| `tkd_promptdata34` | `string` | `varchar` |  |  | TKD_PROMPTDATA34 |
| `tkd_promptdata35` | `string` | `varchar` |  |  | TKD_PROMPTDATA35 |
| `tkd_promptdata36` | `string` | `varchar` |  |  | TKD_PROMPTDATA36 |
| `tkd_promptdata37` | `string` | `varchar` |  |  | TKD_PROMPTDATA37 |
| `tkd_promptdata38` | `string` | `varchar` |  |  | TKD_PROMPTDATA38 |
| `tkd_promptdata39` | `string` | `varchar` |  |  | TKD_PROMPTDATA39 |
| `tkd_promptdata40` | `string` | `varchar` |  |  | TKD_PROMPTDATA40 |
| `tkd_promptdata41` | `string` | `varchar` |  |  | TKD_PROMPTDATA41 |
| `tkd_promptdata42` | `string` | `varchar` |  |  | TKD_PROMPTDATA42 |
| `tkd_promptdata43` | `string` | `varchar` |  |  | TKD_PROMPTDATA43 |
| `tkd_promptdata44` | `string` | `varchar` |  |  | TKD_PROMPTDATA44 |
| `tkd_promptdata45` | `string` | `varchar` |  |  | TKD_PROMPTDATA45 |
| `tkd_promptdata46` | `string` | `varchar` |  |  | TKD_PROMPTDATA46 |
| `tkd_promptdata47` | `string` | `varchar` |  |  | TKD_PROMPTDATA47 |
| `tkd_promptdata48` | `string` | `varchar` |  |  | TKD_PROMPTDATA48 |
| `tkd_promptdata49` | `string` | `varchar` |  |  | TKD_PROMPTDATA49 |
| `tkd_promptdata50` | `string` | `varchar` |  |  | TKD_PROMPTDATA50 |
| `tkd_sessionid` | `float` | `float` |  |  | TKD_SESSIONID |
| `tkd_rstatus` | `string` | `varchar` |  |  | TKD_RSTATUS |
| `tkd_changed` | `string` | `varchar` |  |  | TKD_CHANGED |
| `tkd_promptdata51` | `string` | `varchar` |  |  | TKD_PROMPTDATA51. Max length: 0 |
| `tkd_transid` | `float` | `float` | `PK` | `unique` | TKD_TRANSID |
| `tkd_created` | `timestamp` | `timestamp_ntz` |  |  | TKD_CREATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

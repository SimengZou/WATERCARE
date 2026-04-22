# r5objectservicecodes

eam_R5OBJECTSERVICECODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5objectservicecodes` |
| **Materialization** | `incremental` |
| **Primary keys** | `osc_pk` |
| **Column count** | 61 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `osc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OSC_LASTSAVED |
| `osc_object` | `string` | `varchar` |  |  | OSC_OBJECT |
| `osc_object_org` | `string` | `varchar` |  |  | OSC_OBJECT_ORG |
| `osc_servicecode` | `string` | `varchar` |  |  | OSC_SERVICECODE |
| `osc_servicecode_org` | `string` | `varchar` |  |  | OSC_SERVICECODE_ORG |
| `osc_applytochildren` | `string` | `varchar` |  |  | OSC_APPLYTOCHILDREN |
| `osc_skipthislevel` | `string` | `varchar` |  |  | OSC_SKIPTHISLEVEL |
| `osc_udfchar01` | `string` | `varchar` |  |  | OSC_UDFCHAR01 |
| `osc_udfchar02` | `string` | `varchar` |  |  | OSC_UDFCHAR02 |
| `osc_udfchar03` | `string` | `varchar` |  |  | OSC_UDFCHAR03 |
| `osc_udfchar04` | `string` | `varchar` |  |  | OSC_UDFCHAR04 |
| `osc_udfchar05` | `string` | `varchar` |  |  | OSC_UDFCHAR05 |
| `osc_udfchar06` | `string` | `varchar` |  |  | OSC_UDFCHAR06 |
| `osc_udfchar07` | `string` | `varchar` |  |  | OSC_UDFCHAR07 |
| `osc_udfchar08` | `string` | `varchar` |  |  | OSC_UDFCHAR08 |
| `osc_udfchar09` | `string` | `varchar` |  |  | OSC_UDFCHAR09 |
| `osc_udfchar10` | `string` | `varchar` |  |  | OSC_UDFCHAR10 |
| `osc_udfchar11` | `string` | `varchar` |  |  | OSC_UDFCHAR11 |
| `osc_udfchar12` | `string` | `varchar` |  |  | OSC_UDFCHAR12 |
| `osc_udfchar13` | `string` | `varchar` |  |  | OSC_UDFCHAR13 |
| `osc_udfchar14` | `string` | `varchar` |  |  | OSC_UDFCHAR14 |
| `osc_udfchar15` | `string` | `varchar` |  |  | OSC_UDFCHAR15 |
| `osc_udfchar16` | `string` | `varchar` |  |  | OSC_UDFCHAR16 |
| `osc_udfchar17` | `string` | `varchar` |  |  | OSC_UDFCHAR17 |
| `osc_udfchar18` | `string` | `varchar` |  |  | OSC_UDFCHAR18 |
| `osc_udfchar19` | `string` | `varchar` |  |  | OSC_UDFCHAR19 |
| `osc_udfchar20` | `string` | `varchar` |  |  | OSC_UDFCHAR20 |
| `osc_udfchar21` | `string` | `varchar` |  |  | OSC_UDFCHAR21 |
| `osc_udfchar22` | `string` | `varchar` |  |  | OSC_UDFCHAR22 |
| `osc_udfchar23` | `string` | `varchar` |  |  | OSC_UDFCHAR23 |
| `osc_udfchar24` | `string` | `varchar` |  |  | OSC_UDFCHAR24 |
| `osc_udfchar25` | `string` | `varchar` |  |  | OSC_UDFCHAR25 |
| `osc_udfchar26` | `string` | `varchar` |  |  | OSC_UDFCHAR26 |
| `osc_udfchar27` | `string` | `varchar` |  |  | OSC_UDFCHAR27 |
| `osc_udfchar28` | `string` | `varchar` |  |  | OSC_UDFCHAR28 |
| `osc_udfchar29` | `string` | `varchar` |  |  | OSC_UDFCHAR29 |
| `osc_udfchar30` | `string` | `varchar` |  |  | OSC_UDFCHAR30 |
| `osc_udfnum01` | `float` | `float` |  |  | OSC_UDFNUM01 |
| `osc_udfnum02` | `float` | `float` |  |  | OSC_UDFNUM02 |
| `osc_udfnum03` | `float` | `float` |  |  | OSC_UDFNUM03 |
| `osc_udfnum04` | `float` | `float` |  |  | OSC_UDFNUM04 |
| `osc_udfnum05` | `float` | `float` |  |  | OSC_UDFNUM05 |
| `osc_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | OSC_UDFDATE01 |
| `osc_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | OSC_UDFDATE02 |
| `osc_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | OSC_UDFDATE03 |
| `osc_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | OSC_UDFDATE04 |
| `osc_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | OSC_UDFDATE05 |
| `osc_udfchkbox01` | `string` | `varchar` |  |  | OSC_UDFCHKBOX01 |
| `osc_udfchkbox02` | `string` | `varchar` |  |  | OSC_UDFCHKBOX02 |
| `osc_udfchkbox03` | `string` | `varchar` |  |  | OSC_UDFCHKBOX03 |
| `osc_udfchkbox04` | `string` | `varchar` |  |  | OSC_UDFCHKBOX04 |
| `osc_udfchkbox05` | `string` | `varchar` |  |  | OSC_UDFCHKBOX05 |
| `osc_updatecount` | `float` | `float` |  |  | OSC_UPDATECOUNT |
| `osc_pk` | `string` | `varchar` | `PK` | `unique` | OSC_PK |
| `osc_parent` | `string` | `varchar` |  |  | OSC_PARENT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

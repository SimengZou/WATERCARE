# r5trades

eam_R5TRADES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5trades` |
| **Materialization** | `incremental` |
| **Primary keys** | `trd_code` |
| **Column count** | 64 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `trd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TRD_LASTSAVED |
| `trd_udfchar27` | `string` | `varchar` |  |  | TRD_UDFCHAR27 |
| `trd_udfnum01` | `float` | `float` |  |  | TRD_UDFNUM01 |
| `trd_udfnum02` | `float` | `float` |  |  | TRD_UDFNUM02 |
| `trd_udfnum03` | `float` | `float` |  |  | TRD_UDFNUM03 |
| `trd_udfnum04` | `float` | `float` |  |  | TRD_UDFNUM04 |
| `trd_udfnum05` | `float` | `float` |  |  | TRD_UDFNUM05 |
| `trd_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | TRD_UDFDATE01 |
| `trd_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | TRD_UDFDATE02 |
| `trd_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | TRD_UDFDATE03 |
| `trd_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | TRD_UDFDATE04 |
| `trd_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | TRD_UDFDATE05 |
| `trd_udfchkbox01` | `string` | `varchar` |  |  | TRD_UDFCHKBOX01 |
| `trd_udfchkbox02` | `string` | `varchar` |  |  | TRD_UDFCHKBOX02 |
| `trd_udfchkbox03` | `string` | `varchar` |  |  | TRD_UDFCHKBOX03 |
| `trd_udfchkbox04` | `string` | `varchar` |  |  | TRD_UDFCHKBOX04 |
| `trd_udfchkbox05` | `string` | `varchar` |  |  | TRD_UDFCHKBOX05 |
| `trd_availableforcus` | `string` | `varchar` |  |  | TRD_AVAILABLEFORCUS |
| `trd_udfchar28` | `string` | `varchar` |  |  | TRD_UDFCHAR28 |
| `trd_code` | `string` | `varchar` | `PK` | `unique` | TRD_CODE |
| `trd_desc` | `string` | `varchar` |  |  | TRD_DESC |
| `trd_class` | `string` | `varchar` |  |  | TRD_CLASS |
| `trd_org` | `string` | `varchar` |  |  | TRD_ORG |
| `trd_class_org` | `string` | `varchar` |  |  | TRD_CLASS_ORG |
| `trd_updatecount` | `float` | `float` |  |  | TRD_UPDATECOUNT |
| `trd_created` | `timestamp` | `timestamp_ntz` |  |  | TRD_CREATED |
| `trd_updated` | `timestamp` | `timestamp_ntz` |  |  | TRD_UPDATED |
| `trd_notused` | `string` | `varchar` |  |  | TRD_NOTUSED |
| `trd_laststatusupdate` | `timestamp` | `timestamp_ntz` |  |  | TRD_LASTSTATUSUPDATE |
| `trd_abbreviation` | `string` | `varchar` |  |  | TRD_ABBREVIATION |
| `trd_udfchar01` | `string` | `varchar` |  |  | TRD_UDFCHAR01 |
| `trd_udfchar02` | `string` | `varchar` |  |  | TRD_UDFCHAR02 |
| `trd_udfchar03` | `string` | `varchar` |  |  | TRD_UDFCHAR03 |
| `trd_udfchar04` | `string` | `varchar` |  |  | TRD_UDFCHAR04 |
| `trd_udfchar05` | `string` | `varchar` |  |  | TRD_UDFCHAR05 |
| `trd_udfchar06` | `string` | `varchar` |  |  | TRD_UDFCHAR06 |
| `trd_udfchar07` | `string` | `varchar` |  |  | TRD_UDFCHAR07 |
| `trd_udfchar08` | `string` | `varchar` |  |  | TRD_UDFCHAR08 |
| `trd_udfchar09` | `string` | `varchar` |  |  | TRD_UDFCHAR09 |
| `trd_udfchar10` | `string` | `varchar` |  |  | TRD_UDFCHAR10 |
| `trd_udfchar11` | `string` | `varchar` |  |  | TRD_UDFCHAR11 |
| `trd_udfchar12` | `string` | `varchar` |  |  | TRD_UDFCHAR12 |
| `trd_udfchar13` | `string` | `varchar` |  |  | TRD_UDFCHAR13 |
| `trd_udfchar14` | `string` | `varchar` |  |  | TRD_UDFCHAR14 |
| `trd_udfchar15` | `string` | `varchar` |  |  | TRD_UDFCHAR15 |
| `trd_udfchar16` | `string` | `varchar` |  |  | TRD_UDFCHAR16 |
| `trd_udfchar17` | `string` | `varchar` |  |  | TRD_UDFCHAR17 |
| `trd_udfchar18` | `string` | `varchar` |  |  | TRD_UDFCHAR18 |
| `trd_udfchar19` | `string` | `varchar` |  |  | TRD_UDFCHAR19 |
| `trd_udfchar20` | `string` | `varchar` |  |  | TRD_UDFCHAR20 |
| `trd_udfchar21` | `string` | `varchar` |  |  | TRD_UDFCHAR21 |
| `trd_udfchar22` | `string` | `varchar` |  |  | TRD_UDFCHAR22 |
| `trd_udfchar23` | `string` | `varchar` |  |  | TRD_UDFCHAR23 |
| `trd_udfchar24` | `string` | `varchar` |  |  | TRD_UDFCHAR24 |
| `trd_udfchar25` | `string` | `varchar` |  |  | TRD_UDFCHAR25 |
| `trd_udfchar26` | `string` | `varchar` |  |  | TRD_UDFCHAR26 |
| `trd_udfchar29` | `string` | `varchar` |  |  | TRD_UDFCHAR29 |
| `trd_udfchar30` | `string` | `varchar` |  |  | TRD_UDFCHAR30 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

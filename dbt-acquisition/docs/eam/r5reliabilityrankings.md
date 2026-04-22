# r5reliabilityrankings

eam_R5RELIABILITYRANKINGS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reliabilityrankings` |
| **Materialization** | `incremental` |
| **Primary keys** | `rrk_code`, `rrk_org`, `rrk_revision` |
| **Column count** | 80 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rrk_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RRK_LASTSAVED |
| `rrk_code` | `string` | `varchar` | `PK` |  | RRK_CODE |
| `rrk_org` | `string` | `varchar` | `PK` |  | RRK_ORG |
| `rrk_revision` | `float` | `float` | `PK` |  | RRK_REVISION |
| `rrk_desc` | `string` | `varchar` |  |  | RRK_DESC |
| `rrk_class` | `string` | `varchar` |  |  | RRK_CLASS |
| `rrk_class_org` | `string` | `varchar` |  |  | RRK_CLASS_ORG |
| `rrk_revrstatus` | `string` | `varchar` |  |  | RRK_REVRSTATUS |
| `rrk_revstatus` | `string` | `varchar` |  |  | RRK_REVSTATUS |
| `rrk_created` | `timestamp` | `timestamp_ntz` |  |  | RRK_CREATED |
| `rrk_createdby` | `string` | `varchar` |  |  | RRK_CREATEDBY |
| `rrk_setuplastupdated` | `timestamp` | `timestamp_ntz` |  |  | RRK_SETUPLASTUPDATED |
| `rrk_notused` | `string` | `varchar` |  |  | RRK_NOTUSED |
| `rrk_updatecount` | `float` | `float` |  |  | RRK_UPDATECOUNT |
| `rrk_type` | `string` | `varchar` |  |  | RRK_TYPE |
| `rrk_rtype` | `string` | `varchar` |  |  | RRK_RTYPE |
| `rrk_conditionprotocol` | `string` | `varchar` |  |  | RRK_CONDITIONPROTOCOL |
| `rrk_condscorestart` | `float` | `float` |  |  | RRK_CONDSCORESTART |
| `rrk_condscoreend` | `float` | `float` |  |  | RRK_CONDSCOREEND |
| `rrk_condscorethreshold` | `float` | `float` |  |  | RRK_CONDSCORETHRESHOLD |
| `rrk_decaycurve` | `string` | `varchar` |  |  | RRK_DECAYCURVE |
| `rrk_decaycurve_org` | `string` | `varchar` |  |  | RRK_DECAYCURVE_ORG |
| `rrk_trackhistory` | `string` | `varchar` |  |  | RRK_TRACKHISTORY |
| `rrk_udfchar01` | `string` | `varchar` |  |  | RRK_UDFCHAR01 |
| `rrk_udfchar02` | `string` | `varchar` |  |  | RRK_UDFCHAR02 |
| `rrk_udfchar03` | `string` | `varchar` |  |  | RRK_UDFCHAR03 |
| `rrk_udfchar04` | `string` | `varchar` |  |  | RRK_UDFCHAR04 |
| `rrk_udfchar05` | `string` | `varchar` |  |  | RRK_UDFCHAR05 |
| `rrk_udfchar06` | `string` | `varchar` |  |  | RRK_UDFCHAR06 |
| `rrk_udfchar07` | `string` | `varchar` |  |  | RRK_UDFCHAR07 |
| `rrk_udfchar08` | `string` | `varchar` |  |  | RRK_UDFCHAR08 |
| `rrk_udfchar09` | `string` | `varchar` |  |  | RRK_UDFCHAR09 |
| `rrk_udfchar10` | `string` | `varchar` |  |  | RRK_UDFCHAR10 |
| `rrk_udfchar11` | `string` | `varchar` |  |  | RRK_UDFCHAR11 |
| `rrk_udfchar12` | `string` | `varchar` |  |  | RRK_UDFCHAR12 |
| `rrk_udfchar13` | `string` | `varchar` |  |  | RRK_UDFCHAR13 |
| `rrk_udfchar14` | `string` | `varchar` |  |  | RRK_UDFCHAR14 |
| `rrk_udfchar15` | `string` | `varchar` |  |  | RRK_UDFCHAR15 |
| `rrk_udfchar16` | `string` | `varchar` |  |  | RRK_UDFCHAR16 |
| `rrk_udfchar17` | `string` | `varchar` |  |  | RRK_UDFCHAR17 |
| `rrk_udfchar18` | `string` | `varchar` |  |  | RRK_UDFCHAR18 |
| `rrk_udfchar19` | `string` | `varchar` |  |  | RRK_UDFCHAR19 |
| `rrk_udfchar20` | `string` | `varchar` |  |  | RRK_UDFCHAR20 |
| `rrk_udfchar21` | `string` | `varchar` |  |  | RRK_UDFCHAR21 |
| `rrk_udfchar22` | `string` | `varchar` |  |  | RRK_UDFCHAR22 |
| `rrk_udfchar23` | `string` | `varchar` |  |  | RRK_UDFCHAR23 |
| `rrk_udfchar24` | `string` | `varchar` |  |  | RRK_UDFCHAR24 |
| `rrk_udfchar25` | `string` | `varchar` |  |  | RRK_UDFCHAR25 |
| `rrk_udfchar26` | `string` | `varchar` |  |  | RRK_UDFCHAR26 |
| `rrk_udfchar27` | `string` | `varchar` |  |  | RRK_UDFCHAR27 |
| `rrk_udfchar28` | `string` | `varchar` |  |  | RRK_UDFCHAR28 |
| `rrk_udfchar29` | `string` | `varchar` |  |  | RRK_UDFCHAR29 |
| `rrk_udfchar30` | `string` | `varchar` |  |  | RRK_UDFCHAR30 |
| `rrk_udfnum01` | `float` | `float` |  |  | RRK_UDFNUM01 |
| `rrk_udfnum02` | `float` | `float` |  |  | RRK_UDFNUM02 |
| `rrk_udfnum03` | `float` | `float` |  |  | RRK_UDFNUM03 |
| `rrk_udfnum04` | `float` | `float` |  |  | RRK_UDFNUM04 |
| `rrk_udfnum05` | `float` | `float` |  |  | RRK_UDFNUM05 |
| `rrk_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | RRK_UDFDATE01 |
| `rrk_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | RRK_UDFDATE02 |
| `rrk_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | RRK_UDFDATE03 |
| `rrk_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | RRK_UDFDATE04 |
| `rrk_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | RRK_UDFDATE05 |
| `rrk_udfchkbox01` | `string` | `varchar` |  |  | RRK_UDFCHKBOX01 |
| `rrk_udfchkbox02` | `string` | `varchar` |  |  | RRK_UDFCHKBOX02 |
| `rrk_udfchkbox03` | `string` | `varchar` |  |  | RRK_UDFCHKBOX03 |
| `rrk_udfchkbox04` | `string` | `varchar` |  |  | RRK_UDFCHKBOX04 |
| `rrk_udfchkbox05` | `string` | `varchar` |  |  | RRK_UDFCHKBOX05 |
| `rrk_precision` | `float` | `float` |  |  | RRK_PRECISION |
| `rrk_performanceformula` | `string` | `varchar` |  |  | RRK_PERFORMANCEFORMULA |
| `rrk_performanceformula_org` | `string` | `varchar` |  |  | RRK_PERFORMANCEFORMULA_ORG |
| `rrk_corrscoreranking` | `string` | `varchar` |  |  | RRK_CORRSCORERANKING |
| `rrk_corrscoreranking_org` | `string` | `varchar` |  |  | RRK_CORRSCORERANKING_ORG |
| `rrk_corrscorerankingrev` | `float` | `float` |  |  | RRK_CORRSCORERANKINGREV |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

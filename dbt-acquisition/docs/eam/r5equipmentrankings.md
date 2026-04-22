# r5equipmentrankings

eam_R5EQUIPMENTRANKINGS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5equipmentrankings` |
| **Materialization** | `incremental` |
| **Primary keys** | `eqr_rankingcode`, `eqr_rankingorg`, `eqr_rankingrevision`, `eqr_objcode`, `eqr_objorg` |
| **Column count** | 81 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `eqr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EQR_LASTSAVED |
| `eqr_rankingcode` | `string` | `varchar` | `PK` |  | EQR_RANKINGCODE |
| `eqr_rankingorg` | `string` | `varchar` | `PK` |  | EQR_RANKINGORG |
| `eqr_rankingrevision` | `float` | `float` | `PK` |  | EQR_RANKINGREVISION |
| `eqr_objcode` | `string` | `varchar` | `PK` |  | EQR_OBJCODE |
| `eqr_objorg` | `string` | `varchar` | `PK` |  | EQR_OBJORG |
| `eqr_note` | `string` | `varchar` |  |  | EQR_NOTE |
| `eqr_default` | `string` | `varchar` |  |  | EQR_DEFAULT |
| `eqr_lockrankingvalues` | `string` | `varchar` |  |  | EQR_LOCKRANKINGVALUES |
| `eqr_rankingindex` | `string` | `varchar` |  |  | EQR_RANKINGINDEX |
| `eqr_rankingscore` | `float` | `float` |  |  | EQR_RANKINGSCORE |
| `eqr_valueslastcalculated` | `timestamp` | `timestamp_ntz` |  |  | EQR_VALUESLASTCALCULATED |
| `eqr_surveylastupdated` | `timestamp` | `timestamp_ntz` |  |  | EQR_SURVEYLASTUPDATED |
| `eqr_refreshevery` | `float` | `float` |  |  | EQR_REFRESHEVERY |
| `eqr_refresheveryunit` | `string` | `varchar` |  |  | EQR_REFRESHEVERYUNIT |
| `eqr_nextrefresh` | `timestamp` | `timestamp_ntz` |  |  | EQR_NEXTREFRESH |
| `eqr_refreshsequence` | `float` | `float` |  |  | EQR_REFRESHSEQUENCE |
| `eqr_calculationerror` | `string` | `varchar` |  |  | EQR_CALCULATIONERROR |
| `eqr_created` | `timestamp` | `timestamp_ntz` |  |  | EQR_CREATED |
| `eqr_createdby` | `string` | `varchar` |  |  | EQR_CREATEDBY |
| `eqr_updated` | `timestamp` | `timestamp_ntz` |  |  | EQR_UPDATED |
| `eqr_updatedby` | `string` | `varchar` |  |  | EQR_UPDATEDBY |
| `eqr_updatecount` | `float` | `float` |  |  | EQR_UPDATECOUNT |
| `eqr_udfchar01` | `string` | `varchar` |  |  | EQR_UDFCHAR01 |
| `eqr_udfchar02` | `string` | `varchar` |  |  | EQR_UDFCHAR02 |
| `eqr_udfchar03` | `string` | `varchar` |  |  | EQR_UDFCHAR03 |
| `eqr_udfchar04` | `string` | `varchar` |  |  | EQR_UDFCHAR04 |
| `eqr_udfchar05` | `string` | `varchar` |  |  | EQR_UDFCHAR05 |
| `eqr_udfchar06` | `string` | `varchar` |  |  | EQR_UDFCHAR06 |
| `eqr_udfchar07` | `string` | `varchar` |  |  | EQR_UDFCHAR07 |
| `eqr_udfchar08` | `string` | `varchar` |  |  | EQR_UDFCHAR08 |
| `eqr_udfchar09` | `string` | `varchar` |  |  | EQR_UDFCHAR09 |
| `eqr_udfchar10` | `string` | `varchar` |  |  | EQR_UDFCHAR10 |
| `eqr_udfchar11` | `string` | `varchar` |  |  | EQR_UDFCHAR11 |
| `eqr_udfchar12` | `string` | `varchar` |  |  | EQR_UDFCHAR12 |
| `eqr_udfchar13` | `string` | `varchar` |  |  | EQR_UDFCHAR13 |
| `eqr_udfchar14` | `string` | `varchar` |  |  | EQR_UDFCHAR14 |
| `eqr_udfchar15` | `string` | `varchar` |  |  | EQR_UDFCHAR15 |
| `eqr_udfchar16` | `string` | `varchar` |  |  | EQR_UDFCHAR16 |
| `eqr_udfchar17` | `string` | `varchar` |  |  | EQR_UDFCHAR17 |
| `eqr_udfchar18` | `string` | `varchar` |  |  | EQR_UDFCHAR18 |
| `eqr_udfchar19` | `string` | `varchar` |  |  | EQR_UDFCHAR19 |
| `eqr_udfchar20` | `string` | `varchar` |  |  | EQR_UDFCHAR20 |
| `eqr_udfchar21` | `string` | `varchar` |  |  | EQR_UDFCHAR21 |
| `eqr_udfchar22` | `string` | `varchar` |  |  | EQR_UDFCHAR22 |
| `eqr_udfchar23` | `string` | `varchar` |  |  | EQR_UDFCHAR23 |
| `eqr_udfchar24` | `string` | `varchar` |  |  | EQR_UDFCHAR24 |
| `eqr_udfchar25` | `string` | `varchar` |  |  | EQR_UDFCHAR25 |
| `eqr_udfchar27` | `string` | `varchar` |  |  | EQR_UDFCHAR27 |
| `eqr_udfchar28` | `string` | `varchar` |  |  | EQR_UDFCHAR28 |
| `eqr_udfchar29` | `string` | `varchar` |  |  | EQR_UDFCHAR29 |
| `eqr_udfchar30` | `string` | `varchar` |  |  | EQR_UDFCHAR30 |
| `eqr_udfnum01` | `float` | `float` |  |  | EQR_UDFNUM01 |
| `eqr_udfnum02` | `float` | `float` |  |  | EQR_UDFNUM02 |
| `eqr_udfnum03` | `float` | `float` |  |  | EQR_UDFNUM03 |
| `eqr_udfnum04` | `float` | `float` |  |  | EQR_UDFNUM04 |
| `eqr_udfnum05` | `float` | `float` |  |  | EQR_UDFNUM05 |
| `eqr_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | EQR_UDFDATE01 |
| `eqr_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | EQR_UDFDATE02 |
| `eqr_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | EQR_UDFDATE03 |
| `eqr_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | EQR_UDFDATE04 |
| `eqr_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | EQR_UDFDATE05 |
| `eqr_udfchkbox01` | `string` | `varchar` |  |  | EQR_UDFCHKBOX01 |
| `eqr_udfchkbox02` | `string` | `varchar` |  |  | EQR_UDFCHKBOX02 |
| `eqr_udfchkbox03` | `string` | `varchar` |  |  | EQR_UDFCHKBOX03 |
| `eqr_udfchkbox04` | `string` | `varchar` |  |  | EQR_UDFCHKBOX04 |
| `eqr_udfchkbox05` | `string` | `varchar` |  |  | EQR_UDFCHKBOX05 |
| `eqr_corrscoreranking` | `string` | `varchar` |  |  | EQR_CORRSCORERANKING |
| `eqr_corrscoreranking_org` | `string` | `varchar` |  |  | EQR_CORRSCORERANKING_ORG |
| `eqr_corrscorerankingrev` | `float` | `float` |  |  | EQR_CORRSCORERANKINGREV |
| `eqr_corrscoreindex` | `string` | `varchar` |  |  | EQR_CORRSCOREINDEX |
| `eqr_corrscorescore` | `float` | `float` |  |  | EQR_CORRSCORESCORE |
| `eqr_corrscoredate` | `timestamp` | `timestamp_ntz` |  |  | EQR_CORRSCOREDATE |
| `eqr_corrscorestatus` | `string` | `varchar` |  |  | EQR_CORRSCORESTATUS |
| `eqr_udfchar26` | `string` | `varchar` |  |  | EQR_UDFCHAR26 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

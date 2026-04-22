# r5worksafety

eam_R5WORKSAFETY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5worksafety` |
| **Materialization** | `incremental` |
| **Primary keys** | `ksf_pk` |
| **Column count** | 85 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ksf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | KSF_LASTSAVED |
| `ksf_pk` | `string` | `varchar` | `PK` | `unique` | KSF_PK |
| `ksf_rentity` | `string` | `varchar` |  |  | KSF_RENTITY |
| `ksf_code` | `string` | `varchar` |  |  | KSF_CODE |
| `ksf_hazard` | `string` | `varchar` |  |  | KSF_HAZARD |
| `ksf_hazard_org` | `string` | `varchar` |  |  | KSF_HAZARD_ORG |
| `ksf_hazardrev` | `float` | `float` |  |  | KSF_HAZARDREV |
| `ksf_precaution` | `string` | `varchar` |  |  | KSF_PRECAUTION |
| `ksf_precaution_org` | `string` | `varchar` |  |  | KSF_PRECAUTION_ORG |
| `ksf_precautionrev` | `float` | `float` |  |  | KSF_PRECAUTIONREV |
| `ksf_timing` | `string` | `varchar` |  |  | KSF_TIMING |
| `ksf_validuntil` | `timestamp` | `timestamp_ntz` |  |  | KSF_VALIDUNTIL |
| `ksf_deletepending` | `string` | `varchar` |  |  | KSF_DELETEPENDING |
| `ksf_sequence` | `float` | `float` |  |  | KSF_SEQUENCE |
| `ksf_healthhazard` | `string` | `varchar` |  |  | KSF_HEALTHHAZARD |
| `ksf_flammability` | `string` | `varchar` |  |  | KSF_FLAMMABILITY |
| `ksf_instability` | `string` | `varchar` |  |  | KSF_INSTABILITY |
| `ksf_specialhazards` | `string` | `varchar` |  |  | KSF_SPECIALHAZARDS |
| `ksf_created` | `timestamp` | `timestamp_ntz` |  |  | KSF_CREATED |
| `ksf_createdby` | `string` | `varchar` |  |  | KSF_CREATEDBY |
| `ksf_updated` | `timestamp` | `timestamp_ntz` |  |  | KSF_UPDATED |
| `ksf_updatedby` | `string` | `varchar` |  |  | KSF_UPDATEDBY |
| `ksf_reviewed` | `timestamp` | `timestamp_ntz` |  |  | KSF_REVIEWED |
| `ksf_reviewedby` | `string` | `varchar` |  |  | KSF_REVIEWEDBY |
| `ksf_reviewedname` | `string` | `varchar` |  |  | KSF_REVIEWEDNAME |
| `ksf_reviewedtype` | `string` | `varchar` |  |  | KSF_REVIEWEDTYPE |
| `ksf_sourcerentity` | `string` | `varchar` |  |  | KSF_SOURCERENTITY |
| `ksf_sourcecode` | `string` | `varchar` |  |  | KSF_SOURCECODE |
| `ksf_sourceorg` | `string` | `varchar` |  |  | KSF_SOURCEORG |
| `ksf_sourceupdated` | `timestamp` | `timestamp_ntz` |  |  | KSF_SOURCEUPDATED |
| `ksf_sourceupdatedby` | `string` | `varchar` |  |  | KSF_SOURCEUPDATEDBY |
| `ksf_udfchar01` | `string` | `varchar` |  |  | KSF_UDFCHAR01 |
| `ksf_udfchar02` | `string` | `varchar` |  |  | KSF_UDFCHAR02 |
| `ksf_udfchar03` | `string` | `varchar` |  |  | KSF_UDFCHAR03 |
| `ksf_udfchar04` | `string` | `varchar` |  |  | KSF_UDFCHAR04 |
| `ksf_udfchar05` | `string` | `varchar` |  |  | KSF_UDFCHAR05 |
| `ksf_udfchar06` | `string` | `varchar` |  |  | KSF_UDFCHAR06 |
| `ksf_udfchar07` | `string` | `varchar` |  |  | KSF_UDFCHAR07 |
| `ksf_udfchar08` | `string` | `varchar` |  |  | KSF_UDFCHAR08 |
| `ksf_udfchar09` | `string` | `varchar` |  |  | KSF_UDFCHAR09 |
| `ksf_udfchar10` | `string` | `varchar` |  |  | KSF_UDFCHAR10 |
| `ksf_udfchar11` | `string` | `varchar` |  |  | KSF_UDFCHAR11 |
| `ksf_udfchar12` | `string` | `varchar` |  |  | KSF_UDFCHAR12 |
| `ksf_udfchar13` | `string` | `varchar` |  |  | KSF_UDFCHAR13 |
| `ksf_udfchar14` | `string` | `varchar` |  |  | KSF_UDFCHAR14 |
| `ksf_udfchar15` | `string` | `varchar` |  |  | KSF_UDFCHAR15 |
| `ksf_udfchar16` | `string` | `varchar` |  |  | KSF_UDFCHAR16 |
| `ksf_udfchar17` | `string` | `varchar` |  |  | KSF_UDFCHAR17 |
| `ksf_udfchar18` | `string` | `varchar` |  |  | KSF_UDFCHAR18 |
| `ksf_udfchar19` | `string` | `varchar` |  |  | KSF_UDFCHAR19 |
| `ksf_udfchar20` | `string` | `varchar` |  |  | KSF_UDFCHAR20 |
| `ksf_udfchar21` | `string` | `varchar` |  |  | KSF_UDFCHAR21 |
| `ksf_udfchar22` | `string` | `varchar` |  |  | KSF_UDFCHAR22 |
| `ksf_udfchar23` | `string` | `varchar` |  |  | KSF_UDFCHAR23 |
| `ksf_udfchar24` | `string` | `varchar` |  |  | KSF_UDFCHAR24 |
| `ksf_udfchar25` | `string` | `varchar` |  |  | KSF_UDFCHAR25 |
| `ksf_udfchar26` | `string` | `varchar` |  |  | KSF_UDFCHAR26 |
| `ksf_udfchar27` | `string` | `varchar` |  |  | KSF_UDFCHAR27 |
| `ksf_udfchar28` | `string` | `varchar` |  |  | KSF_UDFCHAR28 |
| `ksf_udfchar29` | `string` | `varchar` |  |  | KSF_UDFCHAR29 |
| `ksf_udfchar30` | `string` | `varchar` |  |  | KSF_UDFCHAR30 |
| `ksf_udfnum01` | `float` | `float` |  |  | KSF_UDFNUM01 |
| `ksf_udfnum02` | `float` | `float` |  |  | KSF_UDFNUM02 |
| `ksf_udfnum03` | `float` | `float` |  |  | KSF_UDFNUM03 |
| `ksf_udfnum04` | `float` | `float` |  |  | KSF_UDFNUM04 |
| `ksf_udfnum05` | `float` | `float` |  |  | KSF_UDFNUM05 |
| `ksf_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | KSF_UDFDATE01 |
| `ksf_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | KSF_UDFDATE02 |
| `ksf_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | KSF_UDFDATE03 |
| `ksf_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | KSF_UDFDATE04 |
| `ksf_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | KSF_UDFDATE05 |
| `ksf_udfchkbox01` | `string` | `varchar` |  |  | KSF_UDFCHKBOX01 |
| `ksf_udfchkbox02` | `string` | `varchar` |  |  | KSF_UDFCHKBOX02 |
| `ksf_udfchkbox03` | `string` | `varchar` |  |  | KSF_UDFCHKBOX03 |
| `ksf_udfchkbox04` | `string` | `varchar` |  |  | KSF_UDFCHKBOX04 |
| `ksf_udfchkbox05` | `string` | `varchar` |  |  | KSF_UDFCHKBOX05 |
| `ksf_updatecount` | `float` | `float` |  |  | KSF_UPDATECOUNT |
| `ksf_precautionapplied` | `string` | `varchar` |  |  | KSF_PRECAUTIONAPPLIED |
| `ksf_responsibility` | `string` | `varchar` |  |  | KSF_RESPONSIBILITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

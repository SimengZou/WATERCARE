# r5entitysafety

eam_R5ENTITYSAFETY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5entitysafety` |
| **Materialization** | `incremental` |
| **Primary keys** | `esf_pk` |
| **Column count** | 78 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `esf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ESF_LASTSAVED |
| `esf_pk` | `string` | `varchar` | `PK` | `unique` | ESF_PK |
| `esf_rentity` | `string` | `varchar` |  |  | ESF_RENTITY |
| `esf_code` | `string` | `varchar` |  |  | ESF_CODE |
| `esf_hazard` | `string` | `varchar` |  |  | ESF_HAZARD |
| `esf_hazard_org` | `string` | `varchar` |  |  | ESF_HAZARD_ORG |
| `esf_precaution` | `string` | `varchar` |  |  | ESF_PRECAUTION |
| `esf_precaution_org` | `string` | `varchar` |  |  | ESF_PRECAUTION_ORG |
| `esf_applytochildren` | `string` | `varchar` |  |  | ESF_APPLYTOCHILDREN |
| `esf_deletepending` | `string` | `varchar` |  |  | ESF_DELETEPENDING |
| `esf_timing` | `string` | `varchar` |  |  | ESF_TIMING |
| `esf_sequence` | `float` | `float` |  |  | ESF_SEQUENCE |
| `esf_healthhazard` | `string` | `varchar` |  |  | ESF_HEALTHHAZARD |
| `esf_flammability` | `string` | `varchar` |  |  | ESF_FLAMMABILITY |
| `esf_instability` | `string` | `varchar` |  |  | ESF_INSTABILITY |
| `esf_specialhazards` | `string` | `varchar` |  |  | ESF_SPECIALHAZARDS |
| `esf_created` | `timestamp` | `timestamp_ntz` |  |  | ESF_CREATED |
| `esf_createdby` | `string` | `varchar` |  |  | ESF_CREATEDBY |
| `esf_updated` | `timestamp` | `timestamp_ntz` |  |  | ESF_UPDATED |
| `esf_updatedby` | `string` | `varchar` |  |  | ESF_UPDATEDBY |
| `esf_reviewed` | `timestamp` | `timestamp_ntz` |  |  | ESF_REVIEWED |
| `esf_reviewedby` | `string` | `varchar` |  |  | ESF_REVIEWEDBY |
| `esf_reviewedname` | `string` | `varchar` |  |  | ESF_REVIEWEDNAME |
| `esf_reviewedtype` | `string` | `varchar` |  |  | ESF_REVIEWEDTYPE |
| `esf_reviewtrigger` | `string` | `varchar` |  |  | ESF_REVIEWTRIGGER |
| `esf_rentcode` | `string` | `varchar` |  |  | ESF_RENTCODE |
| `esf_udfchar01` | `string` | `varchar` |  |  | ESF_UDFCHAR01 |
| `esf_udfchar02` | `string` | `varchar` |  |  | ESF_UDFCHAR02 |
| `esf_udfchar03` | `string` | `varchar` |  |  | ESF_UDFCHAR03 |
| `esf_udfchar04` | `string` | `varchar` |  |  | ESF_UDFCHAR04 |
| `esf_udfchar05` | `string` | `varchar` |  |  | ESF_UDFCHAR05 |
| `esf_udfchar06` | `string` | `varchar` |  |  | ESF_UDFCHAR06 |
| `esf_udfchar07` | `string` | `varchar` |  |  | ESF_UDFCHAR07 |
| `esf_udfchar08` | `string` | `varchar` |  |  | ESF_UDFCHAR08 |
| `esf_udfchar09` | `string` | `varchar` |  |  | ESF_UDFCHAR09 |
| `esf_udfchar10` | `string` | `varchar` |  |  | ESF_UDFCHAR10 |
| `esf_udfchar11` | `string` | `varchar` |  |  | ESF_UDFCHAR11 |
| `esf_udfchar12` | `string` | `varchar` |  |  | ESF_UDFCHAR12 |
| `esf_udfchar13` | `string` | `varchar` |  |  | ESF_UDFCHAR13 |
| `esf_udfchar14` | `string` | `varchar` |  |  | ESF_UDFCHAR14 |
| `esf_udfchar15` | `string` | `varchar` |  |  | ESF_UDFCHAR15 |
| `esf_udfchar16` | `string` | `varchar` |  |  | ESF_UDFCHAR16 |
| `esf_udfchar17` | `string` | `varchar` |  |  | ESF_UDFCHAR17 |
| `esf_udfchar18` | `string` | `varchar` |  |  | ESF_UDFCHAR18 |
| `esf_udfchar19` | `string` | `varchar` |  |  | ESF_UDFCHAR19 |
| `esf_udfchar20` | `string` | `varchar` |  |  | ESF_UDFCHAR20 |
| `esf_udfchar21` | `string` | `varchar` |  |  | ESF_UDFCHAR21 |
| `esf_udfchar22` | `string` | `varchar` |  |  | ESF_UDFCHAR22 |
| `esf_udfchar23` | `string` | `varchar` |  |  | ESF_UDFCHAR23 |
| `esf_udfchar24` | `string` | `varchar` |  |  | ESF_UDFCHAR24 |
| `esf_udfchar25` | `string` | `varchar` |  |  | ESF_UDFCHAR25 |
| `esf_udfchar26` | `string` | `varchar` |  |  | ESF_UDFCHAR26 |
| `esf_udfchar27` | `string` | `varchar` |  |  | ESF_UDFCHAR27 |
| `esf_udfchar28` | `string` | `varchar` |  |  | ESF_UDFCHAR28 |
| `esf_udfchar29` | `string` | `varchar` |  |  | ESF_UDFCHAR29 |
| `esf_udfchar30` | `string` | `varchar` |  |  | ESF_UDFCHAR30 |
| `esf_udfnum01` | `float` | `float` |  |  | ESF_UDFNUM01 |
| `esf_udfnum02` | `float` | `float` |  |  | ESF_UDFNUM02 |
| `esf_udfnum03` | `float` | `float` |  |  | ESF_UDFNUM03 |
| `esf_udfnum04` | `float` | `float` |  |  | ESF_UDFNUM04 |
| `esf_udfnum05` | `float` | `float` |  |  | ESF_UDFNUM05 |
| `esf_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ESF_UDFDATE01 |
| `esf_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ESF_UDFDATE02 |
| `esf_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ESF_UDFDATE03 |
| `esf_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ESF_UDFDATE04 |
| `esf_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ESF_UDFDATE05 |
| `esf_udfchkbox01` | `string` | `varchar` |  |  | ESF_UDFCHKBOX01 |
| `esf_udfchkbox02` | `string` | `varchar` |  |  | ESF_UDFCHKBOX02 |
| `esf_udfchkbox03` | `string` | `varchar` |  |  | ESF_UDFCHKBOX03 |
| `esf_udfchkbox04` | `string` | `varchar` |  |  | ESF_UDFCHKBOX04 |
| `esf_udfchkbox05` | `string` | `varchar` |  |  | ESF_UDFCHKBOX05 |
| `esf_updatecount` | `float` | `float` |  |  | ESF_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

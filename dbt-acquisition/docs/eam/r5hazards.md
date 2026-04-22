# r5hazards

eam_R5HAZARDS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5hazards` |
| **Materialization** | `incremental` |
| **Primary keys** | `haz_code`, `haz_org`, `haz_revision` |
| **Column count** | 73 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `haz_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | HAZ_LASTSAVED |
| `haz_code` | `string` | `varchar` | `PK` |  | HAZ_CODE |
| `haz_org` | `string` | `varchar` | `PK` |  | HAZ_ORG |
| `haz_revision` | `float` | `float` | `PK` |  | HAZ_REVISION |
| `haz_desc` | `string` | `varchar` |  |  | HAZ_DESC |
| `haz_status` | `string` | `varchar` |  |  | HAZ_STATUS |
| `haz_rstatus` | `string` | `varchar` |  |  | HAZ_RSTATUS |
| `haz_notused` | `string` | `varchar` |  |  | HAZ_NOTUSED |
| `haz_type` | `string` | `varchar` |  |  | HAZ_TYPE |
| `haz_class` | `string` | `varchar` |  |  | HAZ_CLASS |
| `haz_class_org` | `string` | `varchar` |  |  | HAZ_CLASS_ORG |
| `haz_reviewrequired` | `timestamp` | `timestamp_ntz` |  |  | HAZ_REVIEWREQUIRED |
| `haz_created` | `timestamp` | `timestamp_ntz` |  |  | HAZ_CREATED |
| `haz_createdby` | `string` | `varchar` |  |  | HAZ_CREATEDBY |
| `haz_updated` | `timestamp` | `timestamp_ntz` |  |  | HAZ_UPDATED |
| `haz_updatedby` | `string` | `varchar` |  |  | HAZ_UPDATEDBY |
| `haz_requested` | `timestamp` | `timestamp_ntz` |  |  | HAZ_REQUESTED |
| `haz_requestedby` | `string` | `varchar` |  |  | HAZ_REQUESTEDBY |
| `haz_approved` | `timestamp` | `timestamp_ntz` |  |  | HAZ_APPROVED |
| `haz_approvedby` | `string` | `varchar` |  |  | HAZ_APPROVEDBY |
| `haz_revisionreason` | `string` | `varchar` |  |  | HAZ_REVISIONREASON |
| `haz_udfchar01` | `string` | `varchar` |  |  | HAZ_UDFCHAR01 |
| `haz_udfchar02` | `string` | `varchar` |  |  | HAZ_UDFCHAR02 |
| `haz_udfchar03` | `string` | `varchar` |  |  | HAZ_UDFCHAR03 |
| `haz_udfchar04` | `string` | `varchar` |  |  | HAZ_UDFCHAR04 |
| `haz_udfchar05` | `string` | `varchar` |  |  | HAZ_UDFCHAR05 |
| `haz_udfchar06` | `string` | `varchar` |  |  | HAZ_UDFCHAR06 |
| `haz_udfchar07` | `string` | `varchar` |  |  | HAZ_UDFCHAR07 |
| `haz_udfchar08` | `string` | `varchar` |  |  | HAZ_UDFCHAR08 |
| `haz_udfchar09` | `string` | `varchar` |  |  | HAZ_UDFCHAR09 |
| `haz_udfchar10` | `string` | `varchar` |  |  | HAZ_UDFCHAR10 |
| `haz_udfchar11` | `string` | `varchar` |  |  | HAZ_UDFCHAR11 |
| `haz_udfchar12` | `string` | `varchar` |  |  | HAZ_UDFCHAR12 |
| `haz_udfchar13` | `string` | `varchar` |  |  | HAZ_UDFCHAR13 |
| `haz_udfchar14` | `string` | `varchar` |  |  | HAZ_UDFCHAR14 |
| `haz_udfchar15` | `string` | `varchar` |  |  | HAZ_UDFCHAR15 |
| `haz_udfchar16` | `string` | `varchar` |  |  | HAZ_UDFCHAR16 |
| `haz_udfchar17` | `string` | `varchar` |  |  | HAZ_UDFCHAR17 |
| `haz_udfchar18` | `string` | `varchar` |  |  | HAZ_UDFCHAR18 |
| `haz_udfchar19` | `string` | `varchar` |  |  | HAZ_UDFCHAR19 |
| `haz_udfchar20` | `string` | `varchar` |  |  | HAZ_UDFCHAR20 |
| `haz_udfchar21` | `string` | `varchar` |  |  | HAZ_UDFCHAR21 |
| `haz_udfchar22` | `string` | `varchar` |  |  | HAZ_UDFCHAR22 |
| `haz_udfchar23` | `string` | `varchar` |  |  | HAZ_UDFCHAR23 |
| `haz_udfchar24` | `string` | `varchar` |  |  | HAZ_UDFCHAR24 |
| `haz_udfchar25` | `string` | `varchar` |  |  | HAZ_UDFCHAR25 |
| `haz_udfchar26` | `string` | `varchar` |  |  | HAZ_UDFCHAR26 |
| `haz_udfchar27` | `string` | `varchar` |  |  | HAZ_UDFCHAR27 |
| `haz_udfchar28` | `string` | `varchar` |  |  | HAZ_UDFCHAR28 |
| `haz_udfchar29` | `string` | `varchar` |  |  | HAZ_UDFCHAR29 |
| `haz_udfchar30` | `string` | `varchar` |  |  | HAZ_UDFCHAR30 |
| `haz_udfnum01` | `float` | `float` |  |  | HAZ_UDFNUM01 |
| `haz_udfnum02` | `float` | `float` |  |  | HAZ_UDFNUM02 |
| `haz_udfnum03` | `float` | `float` |  |  | HAZ_UDFNUM03 |
| `haz_udfnum04` | `float` | `float` |  |  | HAZ_UDFNUM04 |
| `haz_udfnum05` | `float` | `float` |  |  | HAZ_UDFNUM05 |
| `haz_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | HAZ_UDFDATE01 |
| `haz_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | HAZ_UDFDATE02 |
| `haz_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | HAZ_UDFDATE03 |
| `haz_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | HAZ_UDFDATE04 |
| `haz_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | HAZ_UDFDATE05 |
| `haz_udfchkbox01` | `string` | `varchar` |  |  | HAZ_UDFCHKBOX01 |
| `haz_udfchkbox02` | `string` | `varchar` |  |  | HAZ_UDFCHKBOX02 |
| `haz_udfchkbox03` | `string` | `varchar` |  |  | HAZ_UDFCHKBOX03 |
| `haz_udfchkbox04` | `string` | `varchar` |  |  | HAZ_UDFCHKBOX04 |
| `haz_udfchkbox05` | `string` | `varchar` |  |  | HAZ_UDFCHKBOX05 |
| `haz_updatecount` | `float` | `float` |  |  | HAZ_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

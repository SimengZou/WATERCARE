# r5mobilenotebooks

eam_R5MOBILENOTEBOOKS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobilenotebooks` |
| **Materialization** | `incremental` |
| **Primary keys** | `mnb_code` |
| **Column count** | 95 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mnb_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MNB_LASTSAVED |
| `mnb_code` | `string` | `varchar` | `PK` | `unique` | MNB_CODE |
| `mnb_org` | `string` | `varchar` |  |  | MNB_ORG |
| `mnb_class` | `string` | `varchar` |  |  | MNB_CLASS |
| `mnb_class_org` | `string` | `varchar` |  |  | MNB_CLASS_ORG |
| `mnb_private` | `string` | `varchar` |  |  | MNB_PRIVATE |
| `mnb_history` | `string` | `varchar` |  |  | MNB_HISTORY |
| `mnb_note` | `string` | `varchar` |  |  | MNB_NOTE |
| `mnb_defect` | `string` | `varchar` |  |  | MNB_DEFECT |
| `mnb_object` | `string` | `varchar` |  |  | MNB_OBJECT |
| `mnb_object_org` | `string` | `varchar` |  |  | MNB_OBJECT_ORG |
| `mnb_ncfseverity` | `string` | `varchar` |  |  | MNB_NCFSEVERITY |
| `mnb_mtaseverity` | `string` | `varchar` |  |  | MNB_MTASEVERITY |
| `mnb_ncttype` | `string` | `varchar` |  |  | MNB_NCTTYPE |
| `mnb_ncttype_org` | `string` | `varchar` |  |  | MNB_NCTTYPE_ORG |
| `mnb_ncfclass` | `string` | `varchar` |  |  | MNB_NCFCLASS |
| `mnb_ncfclass_org` | `string` | `varchar` |  |  | MNB_NCFCLASS_ORG |
| `mnb_division` | `string` | `varchar` |  |  | MNB_DIVISION |
| `mnb_line` | `string` | `varchar` |  |  | MNB_LINE |
| `mnb_track` | `string` | `varchar` |  |  | MNB_TRACK |
| `mnb_section` | `string` | `varchar` |  |  | MNB_SECTION |
| `mnb_station` | `string` | `varchar` |  |  | MNB_STATION |
| `mnb_stationing_major` | `float` | `float` |  |  | MNB_STATIONING_MAJOR |
| `mnb_stationing_minor` | `float` | `float` |  |  | MNB_STATIONING_MINOR |
| `mnb_detailed_location` | `string` | `varchar` |  |  | MNB_DETAILED_LOCATION |
| `mnb_created` | `timestamp` | `timestamp_ntz` |  |  | MNB_CREATED |
| `mnb_createdby` | `string` | `varchar` |  |  | MNB_CREATEDBY |
| `mnb_mobilecreated` | `timestamp` | `timestamp_ntz` |  |  | MNB_MOBILECREATED |
| `mnb_emailaddress` | `string` | `varchar` |  |  | MNB_EMAILADDRESS |
| `mnb_mnecode` | `string` | `varchar` |  |  | MNB_MNECODE |
| `mnb_udfchar01` | `string` | `varchar` |  |  | MNB_UDFCHAR01 |
| `mnb_udfchar02` | `string` | `varchar` |  |  | MNB_UDFCHAR02 |
| `mnb_udfchar03` | `string` | `varchar` |  |  | MNB_UDFCHAR03 |
| `mnb_udfchar04` | `string` | `varchar` |  |  | MNB_UDFCHAR04 |
| `mnb_udfchar05` | `string` | `varchar` |  |  | MNB_UDFCHAR05 |
| `mnb_udfchar06` | `string` | `varchar` |  |  | MNB_UDFCHAR06 |
| `mnb_udfchar07` | `string` | `varchar` |  |  | MNB_UDFCHAR07 |
| `mnb_udfchar08` | `string` | `varchar` |  |  | MNB_UDFCHAR08 |
| `mnb_udfchar09` | `string` | `varchar` |  |  | MNB_UDFCHAR09 |
| `mnb_udfchar10` | `string` | `varchar` |  |  | MNB_UDFCHAR10 |
| `mnb_udfchar11` | `string` | `varchar` |  |  | MNB_UDFCHAR11 |
| `mnb_udfchar12` | `string` | `varchar` |  |  | MNB_UDFCHAR12 |
| `mnb_udfchar13` | `string` | `varchar` |  |  | MNB_UDFCHAR13 |
| `mnb_udfchar14` | `string` | `varchar` |  |  | MNB_UDFCHAR14 |
| `mnb_udfchar15` | `string` | `varchar` |  |  | MNB_UDFCHAR15 |
| `mnb_udfchar16` | `string` | `varchar` |  |  | MNB_UDFCHAR16 |
| `mnb_udfchar17` | `string` | `varchar` |  |  | MNB_UDFCHAR17 |
| `mnb_udfchar18` | `string` | `varchar` |  |  | MNB_UDFCHAR18 |
| `mnb_udfchar19` | `string` | `varchar` |  |  | MNB_UDFCHAR19 |
| `mnb_udfchar20` | `string` | `varchar` |  |  | MNB_UDFCHAR20 |
| `mnb_udfchar21` | `string` | `varchar` |  |  | MNB_UDFCHAR21 |
| `mnb_udfchar22` | `string` | `varchar` |  |  | MNB_UDFCHAR22 |
| `mnb_udfchar23` | `string` | `varchar` |  |  | MNB_UDFCHAR23 |
| `mnb_udfchar24` | `string` | `varchar` |  |  | MNB_UDFCHAR24 |
| `mnb_udfchar25` | `string` | `varchar` |  |  | MNB_UDFCHAR25 |
| `mnb_udfchar26` | `string` | `varchar` |  |  | MNB_UDFCHAR26 |
| `mnb_udfchar27` | `string` | `varchar` |  |  | MNB_UDFCHAR27 |
| `mnb_udfchar28` | `string` | `varchar` |  |  | MNB_UDFCHAR28 |
| `mnb_udfchar29` | `string` | `varchar` |  |  | MNB_UDFCHAR29 |
| `mnb_udfchar30` | `string` | `varchar` |  |  | MNB_UDFCHAR30 |
| `mnb_udfnum01` | `float` | `float` |  |  | MNB_UDFNUM01 |
| `mnb_udfnum02` | `float` | `float` |  |  | MNB_UDFNUM02 |
| `mnb_udfnum03` | `float` | `float` |  |  | MNB_UDFNUM03 |
| `mnb_udfnum04` | `float` | `float` |  |  | MNB_UDFNUM04 |
| `mnb_udfnum05` | `float` | `float` |  |  | MNB_UDFNUM05 |
| `mnb_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | MNB_UDFDATE01 |
| `mnb_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | MNB_UDFDATE02 |
| `mnb_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | MNB_UDFDATE03 |
| `mnb_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | MNB_UDFDATE04 |
| `mnb_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | MNB_UDFDATE05 |
| `mnb_udfchkbox01` | `string` | `varchar` |  |  | MNB_UDFCHKBOX01 |
| `mnb_udfchkbox02` | `string` | `varchar` |  |  | MNB_UDFCHKBOX02 |
| `mnb_udfchkbox03` | `string` | `varchar` |  |  | MNB_UDFCHKBOX03 |
| `mnb_udfchkbox04` | `string` | `varchar` |  |  | MNB_UDFCHKBOX04 |
| `mnb_udfchkbox05` | `string` | `varchar` |  |  | MNB_UDFCHKBOX05 |
| `mnb_updatecount` | `float` | `float` |  |  | MNB_UPDATECOUNT |
| `mnb_level1` | `string` | `varchar` |  |  | MNB_LEVEL1 |
| `mnb_level1_org` | `string` | `varchar` |  |  | MNB_LEVEL1_ORG |
| `mnb_level2` | `string` | `varchar` |  |  | MNB_LEVEL2 |
| `mnb_level2_org` | `string` | `varchar` |  |  | MNB_LEVEL2_ORG |
| `mnb_level3` | `string` | `varchar` |  |  | MNB_LEVEL3 |
| `mnb_level3_org` | `string` | `varchar` |  |  | MNB_LEVEL3_ORG |
| `mnb_level4` | `string` | `varchar` |  |  | MNB_LEVEL4 |
| `mnb_level4_org` | `string` | `varchar` |  |  | MNB_LEVEL4_ORG |
| `mnb_level3a` | `string` | `varchar` |  |  | MNB_LEVEL3A |
| `mnb_level3a_org` | `string` | `varchar` |  |  | MNB_LEVEL3A_ORG |
| `mnb_updated` | `timestamp` | `timestamp_ntz` |  |  | MNB_UPDATED |
| `mnb_updatedby` | `string` | `varchar` |  |  | MNB_UPDATEDBY |
| `mnb_nonconformity_observation` | `string` | `varchar` |  |  | MNB_NONCONFORMITY_OBSERVATION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

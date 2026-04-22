# r5standworks

eam_R5STANDWORKS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5standworks` |
| **Materialization** | `incremental` |
| **Primary keys** | `stw_code` |
| **Column count** | 128 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `stw_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | STW_LASTSAVED |
| `stw_code` | `string` | `varchar` | `PK` | `unique` | STW_CODE |
| `stw_desc` | `string` | `varchar` |  |  | STW_DESC |
| `stw_class` | `string` | `varchar` |  |  | STW_CLASS |
| `stw_obclass` | `string` | `varchar` |  |  | STW_OBCLASS |
| `stw_duration` | `float` | `float` |  |  | STW_DURATION |
| `stw_jobtype` | `string` | `varchar` |  |  | STW_JOBTYPE |
| `stw_priority` | `string` | `varchar` |  |  | STW_PRIORITY |
| `stw_obtype` | `string` | `varchar` |  |  | STW_OBTYPE |
| `stw_obrtype` | `string` | `varchar` |  |  | STW_OBRTYPE |
| `stw_object` | `string` | `varchar` |  |  | STW_OBJECT |
| `stw_reqm` | `string` | `varchar` |  |  | STW_REQM |
| `stw_org` | `string` | `varchar` |  |  | STW_ORG |
| `stw_class_org` | `string` | `varchar` |  |  | STW_CLASS_ORG |
| `stw_obclass_org` | `string` | `varchar` |  |  | STW_OBCLASS_ORG |
| `stw_object_org` | `string` | `varchar` |  |  | STW_OBJECT_ORG |
| `stw_created` | `timestamp` | `timestamp_ntz` |  |  | STW_CREATED |
| `stw_createdby` | `string` | `varchar` |  |  | STW_CREATEDBY |
| `stw_updated` | `timestamp` | `timestamp_ntz` |  |  | STW_UPDATED |
| `stw_updatedby` | `string` | `varchar` |  |  | STW_UPDATEDBY |
| `stw_woclass` | `string` | `varchar` |  |  | STW_WOCLASS |
| `stw_woclass_org` | `string` | `varchar` |  |  | STW_WOCLASS_ORG |
| `stw_updatecount` | `float` | `float` |  |  | STW_UPDATECOUNT |
| `stw_notused` | `string` | `varchar` |  |  | STW_NOTUSED |
| `stw_type` | `string` | `varchar` |  |  | STW_TYPE |
| `stw_udfchar01` | `string` | `varchar` |  |  | STW_UDFCHAR01 |
| `stw_udfchar02` | `string` | `varchar` |  |  | STW_UDFCHAR02 |
| `stw_udfchar03` | `string` | `varchar` |  |  | STW_UDFCHAR03 |
| `stw_udfchar04` | `string` | `varchar` |  |  | STW_UDFCHAR04 |
| `stw_udfchar05` | `string` | `varchar` |  |  | STW_UDFCHAR05 |
| `stw_udfchar06` | `string` | `varchar` |  |  | STW_UDFCHAR06 |
| `stw_udfchar07` | `string` | `varchar` |  |  | STW_UDFCHAR07 |
| `stw_udfchar08` | `string` | `varchar` |  |  | STW_UDFCHAR08 |
| `stw_udfchar09` | `string` | `varchar` |  |  | STW_UDFCHAR09 |
| `stw_udfchar10` | `string` | `varchar` |  |  | STW_UDFCHAR10 |
| `stw_udfchar11` | `string` | `varchar` |  |  | STW_UDFCHAR11 |
| `stw_udfchar12` | `string` | `varchar` |  |  | STW_UDFCHAR12 |
| `stw_udfchar13` | `string` | `varchar` |  |  | STW_UDFCHAR13 |
| `stw_udfchar14` | `string` | `varchar` |  |  | STW_UDFCHAR14 |
| `stw_udfchar15` | `string` | `varchar` |  |  | STW_UDFCHAR15 |
| `stw_udfchar16` | `string` | `varchar` |  |  | STW_UDFCHAR16 |
| `stw_udfchar17` | `string` | `varchar` |  |  | STW_UDFCHAR17 |
| `stw_udfchar18` | `string` | `varchar` |  |  | STW_UDFCHAR18 |
| `stw_udfchar19` | `string` | `varchar` |  |  | STW_UDFCHAR19 |
| `stw_udfchar20` | `string` | `varchar` |  |  | STW_UDFCHAR20 |
| `stw_udfchar21` | `string` | `varchar` |  |  | STW_UDFCHAR21 |
| `stw_udfchar22` | `string` | `varchar` |  |  | STW_UDFCHAR22 |
| `stw_udfchar23` | `string` | `varchar` |  |  | STW_UDFCHAR23 |
| `stw_udfchar24` | `string` | `varchar` |  |  | STW_UDFCHAR24 |
| `stw_udfchar25` | `string` | `varchar` |  |  | STW_UDFCHAR25 |
| `stw_udfchar26` | `string` | `varchar` |  |  | STW_UDFCHAR26 |
| `stw_udfchar27` | `string` | `varchar` |  |  | STW_UDFCHAR27 |
| `stw_udfchar28` | `string` | `varchar` |  |  | STW_UDFCHAR28 |
| `stw_udfchar29` | `string` | `varchar` |  |  | STW_UDFCHAR29 |
| `stw_udfchar30` | `string` | `varchar` |  |  | STW_UDFCHAR30 |
| `stw_udfnum01` | `float` | `float` |  |  | STW_UDFNUM01 |
| `stw_udfnum02` | `float` | `float` |  |  | STW_UDFNUM02 |
| `stw_udfnum03` | `float` | `float` |  |  | STW_UDFNUM03 |
| `stw_udfnum04` | `float` | `float` |  |  | STW_UDFNUM04 |
| `stw_udfnum05` | `float` | `float` |  |  | STW_UDFNUM05 |
| `stw_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE01 |
| `stw_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE02 |
| `stw_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE03 |
| `stw_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE04 |
| `stw_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE05 |
| `stw_udfchkbox01` | `string` | `varchar` |  |  | STW_UDFCHKBOX01 |
| `stw_udfchkbox02` | `string` | `varchar` |  |  | STW_UDFCHKBOX02 |
| `stw_udfchkbox03` | `string` | `varchar` |  |  | STW_UDFCHKBOX03 |
| `stw_udfchkbox04` | `string` | `varchar` |  |  | STW_UDFCHKBOX04 |
| `stw_udfchkbox05` | `string` | `varchar` |  |  | STW_UDFCHKBOX05 |
| `stw_safetyreviewrequired` | `timestamp` | `timestamp_ntz` |  |  | STW_SAFETYREVIEWREQUIRED |
| `stw_safetyreviewed` | `timestamp` | `timestamp_ntz` |  |  | STW_SAFETYREVIEWED |
| `stw_permitreviewrequired` | `timestamp` | `timestamp_ntz` |  |  | STW_PERMITREVIEWREQUIRED |
| `stw_safetyreviewedby` | `string` | `varchar` |  |  | STW_SAFETYREVIEWEDBY |
| `stw_safetyreviewedname` | `string` | `varchar` |  |  | STW_SAFETYREVIEWEDNAME |
| `stw_safetyreviewedtype` | `string` | `varchar` |  |  | STW_SAFETYREVIEWEDTYPE |
| `stw_permitreviewed` | `timestamp` | `timestamp_ntz` |  |  | STW_PERMITREVIEWED |
| `stw_permitreviewedby` | `string` | `varchar` |  |  | STW_PERMITREVIEWEDBY |
| `stw_permitreviewedname` | `string` | `varchar` |  |  | STW_PERMITREVIEWEDNAME |
| `stw_permitreviewedtype` | `string` | `varchar` |  |  | STW_PERMITREVIEWEDTYPE |
| `stw_udfnote01` | `string` | `varchar` |  |  | STW_UDFNOTE01 |
| `stw_udfnote02` | `string` | `varchar` |  |  | STW_UDFNOTE02 |
| `stw_udfchar31` | `string` | `varchar` |  |  | STW_UDFCHAR31 |
| `stw_udfchar32` | `string` | `varchar` |  |  | STW_UDFCHAR32 |
| `stw_udfchar33` | `string` | `varchar` |  |  | STW_UDFCHAR33 |
| `stw_udfchar34` | `string` | `varchar` |  |  | STW_UDFCHAR34 |
| `stw_udfchar35` | `string` | `varchar` |  |  | STW_UDFCHAR35 |
| `stw_udfchar36` | `string` | `varchar` |  |  | STW_UDFCHAR36 |
| `stw_udfchar37` | `string` | `varchar` |  |  | STW_UDFCHAR37 |
| `stw_udfchar38` | `string` | `varchar` |  |  | STW_UDFCHAR38 |
| `stw_udfchar39` | `string` | `varchar` |  |  | STW_UDFCHAR39 |
| `stw_udfchar40` | `string` | `varchar` |  |  | STW_UDFCHAR40 |
| `stw_udfchar41` | `string` | `varchar` |  |  | STW_UDFCHAR41 |
| `stw_udfchar42` | `string` | `varchar` |  |  | STW_UDFCHAR42 |
| `stw_udfchar43` | `string` | `varchar` |  |  | STW_UDFCHAR43 |
| `stw_udfchar44` | `string` | `varchar` |  |  | STW_UDFCHAR44 |
| `stw_udfchar45` | `string` | `varchar` |  |  | STW_UDFCHAR45 |
| `stw_udfnum06` | `float` | `float` |  |  | STW_UDFNUM06 |
| `stw_udfnum07` | `float` | `float` |  |  | STW_UDFNUM07 |
| `stw_udfnum08` | `float` | `float` |  |  | STW_UDFNUM08 |
| `stw_udfnum09` | `float` | `float` |  |  | STW_UDFNUM09 |
| `stw_udfnum10` | `float` | `float` |  |  | STW_UDFNUM10 |
| `stw_udfdate06` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE06 |
| `stw_udfdate07` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE07 |
| `stw_udfdate08` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE08 |
| `stw_udfdate09` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE09 |
| `stw_udfdate10` | `timestamp` | `timestamp_ntz` |  |  | STW_UDFDATE10 |
| `stw_udfchkbox06` | `string` | `varchar` |  |  | STW_UDFCHKBOX06 |
| `stw_udfchkbox07` | `string` | `varchar` |  |  | STW_UDFCHKBOX07 |
| `stw_udfchkbox08` | `string` | `varchar` |  |  | STW_UDFCHKBOX08 |
| `stw_udfchkbox09` | `string` | `varchar` |  |  | STW_UDFCHKBOX09 |
| `stw_udfchkbox10` | `string` | `varchar` |  |  | STW_UDFCHKBOX10 |
| `stw_udfchar46` | `string` | `varchar` |  |  | STW_UDFCHAR46 |
| `stw_udfchar47` | `string` | `varchar` |  |  | STW_UDFCHAR47 |
| `stw_udfchar48` | `string` | `varchar` |  |  | STW_UDFCHAR48 |
| `stw_udfchar49` | `string` | `varchar` |  |  | STW_UDFCHAR49 |
| `stw_udfchar50` | `string` | `varchar` |  |  | STW_UDFCHAR50 |
| `stw_udfchar51` | `string` | `varchar` |  |  | STW_UDFCHAR51 |
| `stw_udfchar52` | `string` | `varchar` |  |  | STW_UDFCHAR52 |
| `stw_udfchar53` | `string` | `varchar` |  |  | STW_UDFCHAR53 |
| `stw_udfchar54` | `string` | `varchar` |  |  | STW_UDFCHAR54 |
| `stw_udfchar55` | `string` | `varchar` |  |  | STW_UDFCHAR55 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

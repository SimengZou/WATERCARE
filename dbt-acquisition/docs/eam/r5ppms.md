# r5ppms

eam_R5PPMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5ppms` |
| **Materialization** | `incremental` |
| **Primary keys** | `ppm_code`, `ppm_revision` |
| **Column count** | 147 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ppm_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PPM_LASTSAVED |
| `ppm_desc` | `string` | `varchar` |  |  | PPM_DESC |
| `ppm_class` | `string` | `varchar` |  |  | PPM_CLASS |
| `ppm_isstype` | `string` | `varchar` |  |  | PPM_ISSTYPE |
| `ppm_duration` | `float` | `float` |  |  | PPM_DURATION |
| `ppm_freq` | `float` | `float` |  |  | PPM_FREQ |
| `ppm_meter` | `float` | `float` |  |  | PPM_METER |
| `ppm_metuom` | `string` | `varchar` |  |  | PPM_METUOM |
| `ppm_nested` | `string` | `varchar` |  |  | PPM_NESTED |
| `ppm_priority` | `string` | `varchar` |  |  | PPM_PRIORITY |
| `ppm_jobtype` | `string` | `varchar` |  |  | PPM_JOBTYPE |
| `ppm_special` | `string` | `varchar` |  |  | PPM_SPECIAL |
| `ppm_okwindow` | `float` | `float` |  |  | PPM_OKWINDOW |
| `ppm_nearwindow` | `float` | `float` |  |  | PPM_NEARWINDOW |
| `ppm_genwindow` | `float` | `float` |  |  | PPM_GENWINDOW |
| `ppm_revision` | `float` | `float` | `PK` |  | PPM_REVISION |
| `ppm_revrstatus` | `string` | `varchar` |  |  | PPM_REVRSTATUS |
| `ppm_revstatus` | `string` | `varchar` |  |  | PPM_REVSTATUS |
| `ppm_org` | `string` | `varchar` |  |  | PPM_ORG |
| `ppm_class_org` | `string` | `varchar` |  |  | PPM_CLASS_ORG |
| `ppm_package` | `string` | `varchar` |  |  | PPM_PACKAGE |
| `ppm_plan` | `string` | `varchar` |  |  | PPM_PLAN |
| `ppm_notused` | `string` | `varchar` |  |  | PPM_NOTUSED |
| `ppm_estworkload` | `float` | `float` |  |  | PPM_ESTWORKLOAD |
| `ppm_woclass` | `string` | `varchar` |  |  | PPM_WOCLASS |
| `ppm_woclass_org` | `string` | `varchar` |  |  | PPM_WOCLASS_ORG |
| `ppm_nestedpmstatus` | `string` | `varchar` |  |  | PPM_NESTEDPMSTATUS |
| `ppm_code` | `string` | `varchar` | `PK` |  | PPM_CODE |
| `ppm_perioduom` | `string` | `varchar` |  |  | PPM_PERIODUOM |
| `ppm_nestedtolmin` | `float` | `float` |  |  | PPM_NESTEDTOLMIN |
| `ppm_nestedtolmax` | `float` | `float` |  |  | PPM_NESTEDTOLMAX |
| `ppm_nestedtolmetmin` | `float` | `float` |  |  | PPM_NESTEDTOLMETMIN |
| `ppm_nestedtolmetmax` | `float` | `float` |  |  | PPM_NESTEDTOLMETMAX |
| `ppm_nestedtolmet2min` | `float` | `float` |  |  | PPM_NESTEDTOLMET2MIN |
| `ppm_nestedtolmet2max` | `float` | `float` |  |  | PPM_NESTEDTOLMET2MAX |
| `ppm_meter2` | `float` | `float` |  |  | PPM_METER2 |
| `ppm_metuom2` | `string` | `varchar` |  |  | PPM_METUOM2 |
| `ppm_updatecount` | `float` | `float` |  |  | PPM_UPDATECOUNT |
| `ppm_schedgrp` | `string` | `varchar` |  |  | PPM_SCHEDGRP |
| `ppm_performonweek` | `string` | `varchar` |  |  | PPM_PERFORMONWEEK |
| `ppm_performonday` | `float` | `float` |  |  | PPM_PERFORMONDAY |
| `ppm_prodpriority` | `string` | `varchar` |  |  | PPM_PRODPRIORITY |
| `ppm_requestedstartdatebuffer` | `float` | `float` |  |  | PPM_REQUESTEDSTARTDATEBUFFER |
| `ppm_requestedenddatebuffer` | `float` | `float` |  |  | PPM_REQUESTEDENDDATEBUFFER |
| `ppm_udfchar01` | `string` | `varchar` |  |  | PPM_UDFCHAR01 |
| `ppm_udfchar02` | `string` | `varchar` |  |  | PPM_UDFCHAR02 |
| `ppm_udfchar03` | `string` | `varchar` |  |  | PPM_UDFCHAR03 |
| `ppm_udfchar04` | `string` | `varchar` |  |  | PPM_UDFCHAR04 |
| `ppm_udfchar05` | `string` | `varchar` |  |  | PPM_UDFCHAR05 |
| `ppm_udfchar06` | `string` | `varchar` |  |  | PPM_UDFCHAR06 |
| `ppm_udfchar07` | `string` | `varchar` |  |  | PPM_UDFCHAR07 |
| `ppm_udfchar08` | `string` | `varchar` |  |  | PPM_UDFCHAR08 |
| `ppm_udfchar09` | `string` | `varchar` |  |  | PPM_UDFCHAR09 |
| `ppm_udfchar10` | `string` | `varchar` |  |  | PPM_UDFCHAR10 |
| `ppm_udfchar11` | `string` | `varchar` |  |  | PPM_UDFCHAR11 |
| `ppm_udfchar12` | `string` | `varchar` |  |  | PPM_UDFCHAR12 |
| `ppm_udfchar13` | `string` | `varchar` |  |  | PPM_UDFCHAR13 |
| `ppm_udfchar14` | `string` | `varchar` |  |  | PPM_UDFCHAR14 |
| `ppm_udfchar15` | `string` | `varchar` |  |  | PPM_UDFCHAR15 |
| `ppm_udfchar16` | `string` | `varchar` |  |  | PPM_UDFCHAR16 |
| `ppm_udfchar17` | `string` | `varchar` |  |  | PPM_UDFCHAR17 |
| `ppm_udfchar18` | `string` | `varchar` |  |  | PPM_UDFCHAR18 |
| `ppm_udfchar19` | `string` | `varchar` |  |  | PPM_UDFCHAR19 |
| `ppm_udfchar20` | `string` | `varchar` |  |  | PPM_UDFCHAR20 |
| `ppm_udfchar21` | `string` | `varchar` |  |  | PPM_UDFCHAR21 |
| `ppm_udfchar22` | `string` | `varchar` |  |  | PPM_UDFCHAR22 |
| `ppm_udfchar23` | `string` | `varchar` |  |  | PPM_UDFCHAR23 |
| `ppm_udfchar24` | `string` | `varchar` |  |  | PPM_UDFCHAR24 |
| `ppm_udfchar25` | `string` | `varchar` |  |  | PPM_UDFCHAR25 |
| `ppm_udfchar26` | `string` | `varchar` |  |  | PPM_UDFCHAR26 |
| `ppm_udfchar27` | `string` | `varchar` |  |  | PPM_UDFCHAR27 |
| `ppm_udfchar28` | `string` | `varchar` |  |  | PPM_UDFCHAR28 |
| `ppm_udfchar29` | `string` | `varchar` |  |  | PPM_UDFCHAR29 |
| `ppm_udfchar30` | `string` | `varchar` |  |  | PPM_UDFCHAR30 |
| `ppm_udfnum01` | `float` | `float` |  |  | PPM_UDFNUM01 |
| `ppm_udfnum02` | `float` | `float` |  |  | PPM_UDFNUM02 |
| `ppm_udfnum03` | `float` | `float` |  |  | PPM_UDFNUM03 |
| `ppm_udfnum04` | `float` | `float` |  |  | PPM_UDFNUM04 |
| `ppm_udfnum05` | `float` | `float` |  |  | PPM_UDFNUM05 |
| `ppm_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE01 |
| `ppm_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE02 |
| `ppm_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE03 |
| `ppm_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE04 |
| `ppm_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE05 |
| `ppm_udfchkbox01` | `string` | `varchar` |  |  | PPM_UDFCHKBOX01 |
| `ppm_udfchkbox02` | `string` | `varchar` |  |  | PPM_UDFCHKBOX02 |
| `ppm_udfchkbox03` | `string` | `varchar` |  |  | PPM_UDFCHKBOX03 |
| `ppm_udfchkbox04` | `string` | `varchar` |  |  | PPM_UDFCHKBOX04 |
| `ppm_udfchkbox05` | `string` | `varchar` |  |  | PPM_UDFCHKBOX05 |
| `ppm_safetyreviewrequired` | `timestamp` | `timestamp_ntz` |  |  | PPM_SAFETYREVIEWREQUIRED |
| `ppm_safetyreviewed` | `timestamp` | `timestamp_ntz` |  |  | PPM_SAFETYREVIEWED |
| `ppm_safetyreviewedby` | `string` | `varchar` |  |  | PPM_SAFETYREVIEWEDBY |
| `ppm_safetyreviewedname` | `string` | `varchar` |  |  | PPM_SAFETYREVIEWEDNAME |
| `ppm_permitreviewedby` | `string` | `varchar` |  |  | PPM_PERMITREVIEWEDBY |
| `ppm_safetyreviewedtype` | `string` | `varchar` |  |  | PPM_SAFETYREVIEWEDTYPE |
| `ppm_permitreviewrequired` | `timestamp` | `timestamp_ntz` |  |  | PPM_PERMITREVIEWREQUIRED |
| `ppm_permitreviewed` | `timestamp` | `timestamp_ntz` |  |  | PPM_PERMITREVIEWED |
| `ppm_permitreviewedname` | `string` | `varchar` |  |  | PPM_PERMITREVIEWEDNAME |
| `ppm_permitreviewedtype` | `string` | `varchar` |  |  | PPM_PERMITREVIEWEDTYPE |
| `ppm_udfnote01` | `string` | `varchar` |  |  | PPM_UDFNOTE01 |
| `ppm_udfnote02` | `string` | `varchar` |  |  | PPM_UDFNOTE02 |
| `ppm_udfchar31` | `string` | `varchar` |  |  | PPM_UDFCHAR31 |
| `ppm_udfchar32` | `string` | `varchar` |  |  | PPM_UDFCHAR32 |
| `ppm_udfchar33` | `string` | `varchar` |  |  | PPM_UDFCHAR33 |
| `ppm_udfchar34` | `string` | `varchar` |  |  | PPM_UDFCHAR34 |
| `ppm_udfchar35` | `string` | `varchar` |  |  | PPM_UDFCHAR35 |
| `ppm_udfchar36` | `string` | `varchar` |  |  | PPM_UDFCHAR36 |
| `ppm_udfchar37` | `string` | `varchar` |  |  | PPM_UDFCHAR37 |
| `ppm_udfchar38` | `string` | `varchar` |  |  | PPM_UDFCHAR38 |
| `ppm_udfchar39` | `string` | `varchar` |  |  | PPM_UDFCHAR39 |
| `ppm_udfchar40` | `string` | `varchar` |  |  | PPM_UDFCHAR40 |
| `ppm_udfchar41` | `string` | `varchar` |  |  | PPM_UDFCHAR41 |
| `ppm_udfchar42` | `string` | `varchar` |  |  | PPM_UDFCHAR42 |
| `ppm_udfchar43` | `string` | `varchar` |  |  | PPM_UDFCHAR43 |
| `ppm_udfchar44` | `string` | `varchar` |  |  | PPM_UDFCHAR44 |
| `ppm_udfchar45` | `string` | `varchar` |  |  | PPM_UDFCHAR45 |
| `ppm_udfnum06` | `float` | `float` |  |  | PPM_UDFNUM06 |
| `ppm_udfnum07` | `float` | `float` |  |  | PPM_UDFNUM07 |
| `ppm_udfnum08` | `float` | `float` |  |  | PPM_UDFNUM08 |
| `ppm_udfnum09` | `float` | `float` |  |  | PPM_UDFNUM09 |
| `ppm_udfnum10` | `float` | `float` |  |  | PPM_UDFNUM10 |
| `ppm_udfdate06` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE06 |
| `ppm_udfdate07` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE07 |
| `ppm_udfdate08` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE08 |
| `ppm_udfdate09` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE09 |
| `ppm_udfdate10` | `timestamp` | `timestamp_ntz` |  |  | PPM_UDFDATE10 |
| `ppm_udfchkbox06` | `string` | `varchar` |  |  | PPM_UDFCHKBOX06 |
| `ppm_udfchkbox07` | `string` | `varchar` |  |  | PPM_UDFCHKBOX07 |
| `ppm_udfchkbox08` | `string` | `varchar` |  |  | PPM_UDFCHKBOX08 |
| `ppm_udfchar47` | `string` | `varchar` |  |  | PPM_UDFCHAR47 |
| `ppm_udfchkbox09` | `string` | `varchar` |  |  | PPM_UDFCHKBOX09 |
| `ppm_udfchkbox10` | `string` | `varchar` |  |  | PPM_UDFCHKBOX10 |
| `ppm_udfchar46` | `string` | `varchar` |  |  | PPM_UDFCHAR46 |
| `ppm_udfchar48` | `string` | `varchar` |  |  | PPM_UDFCHAR48 |
| `ppm_udfchar49` | `string` | `varchar` |  |  | PPM_UDFCHAR49 |
| `ppm_udfchar50` | `string` | `varchar` |  |  | PPM_UDFCHAR50 |
| `ppm_udfchar51` | `string` | `varchar` |  |  | PPM_UDFCHAR51 |
| `ppm_udfchar52` | `string` | `varchar` |  |  | PPM_UDFCHAR52 |
| `ppm_udfchar53` | `string` | `varchar` |  |  | PPM_UDFCHAR53 |
| `ppm_udfchar54` | `string` | `varchar` |  |  | PPM_UDFCHAR54 |
| `ppm_udfchar55` | `string` | `varchar` |  |  | PPM_UDFCHAR55 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5ppmobjects

eam_R5PPMOBJECTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5ppmobjects` |
| **Materialization** | `incremental` |
| **Primary keys** | `ppo_pk`, `ppo_revision` |
| **Column count** | 144 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ppo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PPO_LASTSAVED |
| `ppo_pk` | `float` | `float` | `PK` |  | PPO_PK |
| `ppo_ppm` | `string` | `varchar` |  |  | PPO_PPM |
| `ppo_revision` | `float` | `float` | `PK` |  | PPO_REVISION |
| `ppo_obtype` | `string` | `varchar` |  |  | PPO_OBTYPE |
| `ppo_obrtype` | `string` | `varchar` |  |  | PPO_OBRTYPE |
| `ppo_location` | `string` | `varchar` |  |  | PPO_LOCATION |
| `ppo_mrc` | `string` | `varchar` |  |  | PPO_MRC |
| `ppo_costcode` | `string` | `varchar` |  |  | PPO_COSTCODE |
| `ppo_route` | `string` | `varchar` |  |  | PPO_ROUTE |
| `ppo_due` | `timestamp` | `timestamp_ntz` |  |  | PPO_DUE |
| `ppo_metuom` | `string` | `varchar` |  |  | PPO_METUOM |
| `ppo_meterdue` | `float` | `float` |  |  | PPO_METERDUE |
| `ppo_freq` | `float` | `float` |  |  | PPO_FREQ |
| `ppo_meter` | `float` | `float` |  |  | PPO_METER |
| `ppo_isstype` | `string` | `varchar` |  |  | PPO_ISSTYPE |
| `ppo_fixed` | `string` | `varchar` |  |  | PPO_FIXED |
| `ppo_deactive` | `timestamp` | `timestamp_ntz` |  |  | PPO_DEACTIVE |
| `ppo_changed` | `string` | `varchar` |  |  | PPO_CHANGED |
| `ppo_class` | `string` | `varchar` |  |  | PPO_CLASS |
| `ppo_class_org` | `string` | `varchar` |  |  | PPO_CLASS_ORG |
| `ppo_object_org` | `string` | `varchar` |  |  | PPO_OBJECT_ORG |
| `ppo_location_org` | `string` | `varchar` |  |  | PPO_LOCATION_ORG |
| `ppo_org` | `string` | `varchar` |  |  | PPO_ORG |
| `ppo_package` | `string` | `varchar` |  |  | PPO_PACKAGE |
| `ppo_person` | `string` | `varchar` |  |  | PPO_PERSON |
| `ppo_perioduom` | `string` | `varchar` |  |  | PPO_PERIODUOM |
| `ppo_meterdue2` | `float` | `float` |  |  | PPO_METERDUE2 |
| `ppo_metuom2` | `string` | `varchar` |  |  | PPO_METUOM2 |
| `ppo_meter2` | `float` | `float` |  |  | PPO_METER2 |
| `ppo_updatecount` | `float` | `float` |  |  | PPO_UPDATECOUNT |
| `ppo_dormstart` | `timestamp` | `timestamp_ntz` |  |  | PPO_DORMSTART |
| `ppo_dormend` | `timestamp` | `timestamp_ntz` |  |  | PPO_DORMEND |
| `ppo_dormreuse` | `string` | `varchar` |  |  | PPO_DORMREUSE |
| `ppo_schedgrp` | `string` | `varchar` |  |  | PPO_SCHEDGRP |
| `ppo_set` | `string` | `varchar` |  |  | PPO_SET |
| `ppo_frompoint` | `float` | `float` |  |  | PPO_FROMPOINT |
| `ppo_fromrefdesc` | `string` | `varchar` |  |  | PPO_FROMREFDESC |
| `ppo_fromgeoref` | `string` | `varchar` |  |  | PPO_FROMGEOREF |
| `ppo_topoint` | `float` | `float` |  |  | PPO_TOPOINT |
| `ppo_torefdesc` | `string` | `varchar` |  |  | PPO_TOREFDESC |
| `ppo_togeoref` | `string` | `varchar` |  |  | PPO_TOGEOREF |
| `ppo_lockedbysession` | `float` | `float` |  |  | PPO_LOCKEDBYSESSION |
| `ppo_performonweek` | `string` | `varchar` |  |  | PPO_PERFORMONWEEK |
| `ppo_performonday` | `float` | `float` |  |  | PPO_PERFORMONDAY |
| `ppo_setid` | `float` | `float` |  |  | PPO_SETID |
| `ppo_includenonconformities` | `string` | `varchar` |  |  | PPO_INCLUDENONCONFORMITIES |
| `ppo_duenonconformitiesonly` | `string` | `varchar` |  |  | PPO_DUENONCONFORMITIESONLY |
| `ppo_udfchar01` | `string` | `varchar` |  |  | PPO_UDFCHAR01 |
| `ppo_udfchar02` | `string` | `varchar` |  |  | PPO_UDFCHAR02 |
| `ppo_udfchar03` | `string` | `varchar` |  |  | PPO_UDFCHAR03 |
| `ppo_udfchar04` | `string` | `varchar` |  |  | PPO_UDFCHAR04 |
| `ppo_udfchar05` | `string` | `varchar` |  |  | PPO_UDFCHAR05 |
| `ppo_udfchar06` | `string` | `varchar` |  |  | PPO_UDFCHAR06 |
| `ppo_udfchar07` | `string` | `varchar` |  |  | PPO_UDFCHAR07 |
| `ppo_udfchar08` | `string` | `varchar` |  |  | PPO_UDFCHAR08 |
| `ppo_udfchar09` | `string` | `varchar` |  |  | PPO_UDFCHAR09 |
| `ppo_udfchar10` | `string` | `varchar` |  |  | PPO_UDFCHAR10 |
| `ppo_udfchar11` | `string` | `varchar` |  |  | PPO_UDFCHAR11 |
| `ppo_object` | `string` | `varchar` |  |  | PPO_OBJECT |
| `ppo_udfchar12` | `string` | `varchar` |  |  | PPO_UDFCHAR12 |
| `ppo_udfchar13` | `string` | `varchar` |  |  | PPO_UDFCHAR13 |
| `ppo_udfchar14` | `string` | `varchar` |  |  | PPO_UDFCHAR14 |
| `ppo_udfchar15` | `string` | `varchar` |  |  | PPO_UDFCHAR15 |
| `ppo_udfchar16` | `string` | `varchar` |  |  | PPO_UDFCHAR16 |
| `ppo_udfchar17` | `string` | `varchar` |  |  | PPO_UDFCHAR17 |
| `ppo_udfchar18` | `string` | `varchar` |  |  | PPO_UDFCHAR18 |
| `ppo_udfchar19` | `string` | `varchar` |  |  | PPO_UDFCHAR19 |
| `ppo_udfchar20` | `string` | `varchar` |  |  | PPO_UDFCHAR20 |
| `ppo_udfchar21` | `string` | `varchar` |  |  | PPO_UDFCHAR21 |
| `ppo_udfchar22` | `string` | `varchar` |  |  | PPO_UDFCHAR22 |
| `ppo_udfchar23` | `string` | `varchar` |  |  | PPO_UDFCHAR23 |
| `ppo_udfchar24` | `string` | `varchar` |  |  | PPO_UDFCHAR24 |
| `ppo_udfchar25` | `string` | `varchar` |  |  | PPO_UDFCHAR25 |
| `ppo_udfchar26` | `string` | `varchar` |  |  | PPO_UDFCHAR26 |
| `ppo_udfchar27` | `string` | `varchar` |  |  | PPO_UDFCHAR27 |
| `ppo_udfchar28` | `string` | `varchar` |  |  | PPO_UDFCHAR28 |
| `ppo_udfchar29` | `string` | `varchar` |  |  | PPO_UDFCHAR29 |
| `ppo_udfchar30` | `string` | `varchar` |  |  | PPO_UDFCHAR30 |
| `ppo_udfchar31` | `string` | `varchar` |  |  | PPO_UDFCHAR31 |
| `ppo_udfchar32` | `string` | `varchar` |  |  | PPO_UDFCHAR32 |
| `ppo_udfchar33` | `string` | `varchar` |  |  | PPO_UDFCHAR33 |
| `ppo_udfchar34` | `string` | `varchar` |  |  | PPO_UDFCHAR34 |
| `ppo_udfchar35` | `string` | `varchar` |  |  | PPO_UDFCHAR35 |
| `ppo_udfchar36` | `string` | `varchar` |  |  | PPO_UDFCHAR36 |
| `ppo_udfchar37` | `string` | `varchar` |  |  | PPO_UDFCHAR37 |
| `ppo_udfchar38` | `string` | `varchar` |  |  | PPO_UDFCHAR38 |
| `ppo_udfchar39` | `string` | `varchar` |  |  | PPO_UDFCHAR39 |
| `ppo_udfchar40` | `string` | `varchar` |  |  | PPO_UDFCHAR40 |
| `ppo_udfchar41` | `string` | `varchar` |  |  | PPO_UDFCHAR41 |
| `ppo_udfchar42` | `string` | `varchar` |  |  | PPO_UDFCHAR42 |
| `ppo_udfchar43` | `string` | `varchar` |  |  | PPO_UDFCHAR43 |
| `ppo_udfchar44` | `string` | `varchar` |  |  | PPO_UDFCHAR44 |
| `ppo_udfchar45` | `string` | `varchar` |  |  | PPO_UDFCHAR45 |
| `ppo_udfnum01` | `float` | `float` |  |  | PPO_UDFNUM01 |
| `ppo_udfnum02` | `float` | `float` |  |  | PPO_UDFNUM02 |
| `ppo_udfnum03` | `float` | `float` |  |  | PPO_UDFNUM03 |
| `ppo_udfnum04` | `float` | `float` |  |  | PPO_UDFNUM04 |
| `ppo_udfnum05` | `float` | `float` |  |  | PPO_UDFNUM05 |
| `ppo_udfnum07` | `float` | `float` |  |  | PPO_UDFNUM07 |
| `ppo_udfnum08` | `float` | `float` |  |  | PPO_UDFNUM08 |
| `ppo_udfnum09` | `float` | `float` |  |  | PPO_UDFNUM09 |
| `ppo_udfnum10` | `float` | `float` |  |  | PPO_UDFNUM10 |
| `ppo_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE01 |
| `ppo_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE02 |
| `ppo_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE03 |
| `ppo_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE04 |
| `ppo_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE05 |
| `ppo_udfdate06` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE06 |
| `ppo_udfdate07` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE07 |
| `ppo_udfdate08` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE08 |
| `ppo_udfdate09` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE09 |
| `ppo_udfdate10` | `timestamp` | `timestamp_ntz` |  |  | PPO_UDFDATE10 |
| `ppo_udfchkbox01` | `string` | `varchar` |  |  | PPO_UDFCHKBOX01 |
| `ppo_udfchkbox02` | `string` | `varchar` |  |  | PPO_UDFCHKBOX02 |
| `ppo_udfchkbox03` | `string` | `varchar` |  |  | PPO_UDFCHKBOX03 |
| `ppo_udfchkbox04` | `string` | `varchar` |  |  | PPO_UDFCHKBOX04 |
| `ppo_udfchkbox05` | `string` | `varchar` |  |  | PPO_UDFCHKBOX05 |
| `ppo_udfchkbox06` | `string` | `varchar` |  |  | PPO_UDFCHKBOX06 |
| `ppo_udfchkbox07` | `string` | `varchar` |  |  | PPO_UDFCHKBOX07 |
| `ppo_udfchkbox08` | `string` | `varchar` |  |  | PPO_UDFCHKBOX08 |
| `ppo_udfchkbox09` | `string` | `varchar` |  |  | PPO_UDFCHKBOX09 |
| `ppo_udfchkbox10` | `string` | `varchar` |  |  | PPO_UDFCHKBOX10 |
| `ppo_udfnote01` | `string` | `varchar` |  |  | PPO_UDFNOTE01 |
| `ppo_udfnote02` | `string` | `varchar` |  |  | PPO_UDFNOTE02 |
| `ppo_shift` | `string` | `varchar` |  |  | PPO_SHIFT |
| `ppo_copyudfstowo` | `string` | `varchar` |  |  | PPO_COPYUDFSTOWO |
| `ppo_udfchar46` | `string` | `varchar` |  |  | PPO_UDFCHAR46 |
| `ppo_udfchar47` | `string` | `varchar` |  |  | PPO_UDFCHAR47 |
| `ppo_udfchar48` | `string` | `varchar` |  |  | PPO_UDFCHAR48 |
| `ppo_udfchar49` | `string` | `varchar` |  |  | PPO_UDFCHAR49 |
| `ppo_udfchar50` | `string` | `varchar` |  |  | PPO_UDFCHAR50 |
| `ppo_udfchar51` | `string` | `varchar` |  |  | PPO_UDFCHAR51 |
| `ppo_udfchar52` | `string` | `varchar` |  |  | PPO_UDFCHAR52 |
| `ppo_udfchar53` | `string` | `varchar` |  |  | PPO_UDFCHAR53 |
| `ppo_udfchar54` | `string` | `varchar` |  |  | PPO_UDFCHAR54 |
| `ppo_udfchar55` | `string` | `varchar` |  |  | PPO_UDFCHAR55 |
| `ppo_udfnum06` | `float` | `float` |  |  | PPO_UDFNUM06 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

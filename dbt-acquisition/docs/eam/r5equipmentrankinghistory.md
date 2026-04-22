# r5equipmentrankinghistory

eam_R5EQUIPMENTRANKINGHISTORY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5equipmentrankinghistory` |
| **Materialization** | `incremental` |
| **Primary keys** | `erh_pk` |
| **Column count** | 117 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `erh_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ERH_LASTSAVED |
| `erh_rankingcode` | `string` | `varchar` |  |  | ERH_RANKINGCODE |
| `erh_rankingorg` | `string` | `varchar` |  |  | ERH_RANKINGORG |
| `erh_rankingrevision` | `float` | `float` |  |  | ERH_RANKINGREVISION |
| `erh_rankingtype` | `string` | `varchar` |  |  | ERH_RANKINGTYPE |
| `erh_objcode` | `string` | `varchar` |  |  | ERH_OBJCODE |
| `erh_objorg` | `string` | `varchar` |  |  | ERH_OBJORG |
| `erh_date` | `timestamp` | `timestamp_ntz` |  |  | ERH_DATE |
| `erh_score` | `float` | `float` |  |  | ERH_SCORE |
| `erh_index` | `string` | `varchar` |  |  | ERH_INDEX |
| `erh_endofusefullife` | `timestamp` | `timestamp_ntz` |  |  | ERH_ENDOFUSEFULLIFE |
| `erh_conditionprotocol` | `string` | `varchar` |  |  | ERH_CONDITIONPROTOCOL |
| `erh_condscorestart` | `float` | `float` |  |  | ERH_CONDSCORESTART |
| `erh_condscoreend` | `float` | `float` |  |  | ERH_CONDSCOREEND |
| `erh_condscorethreshold` | `float` | `float` |  |  | ERH_CONDSCORETHRESHOLD |
| `erh_decaycurve` | `string` | `varchar` |  |  | ERH_DECAYCURVE |
| `erh_decaycurve_org` | `string` | `varchar` |  |  | ERH_DECAYCURVE_ORG |
| `erh_corrconditionscore` | `float` | `float` |  |  | ERH_CORRCONDITIONSCORE |
| `erh_corrconditionreason` | `string` | `varchar` |  |  | ERH_CORRCONDITIONREASON |
| `erh_corrconditiondate` | `timestamp` | `timestamp_ntz` |  |  | ERH_CORRCONDITIONDATE |
| `erh_corrconditionusage` | `float` | `float` |  |  | ERH_CORRCONDITIONUSAGE |
| `erh_primaryuom` | `string` | `varchar` |  |  | ERH_PRIMARYUOM |
| `erh_servicelife` | `float` | `float` |  |  | ERH_SERVICELIFE |
| `erh_dailyusageuom` | `string` | `varchar` |  |  | ERH_DAILYUSAGEUOM |
| `erh_usage` | `float` | `float` |  |  | ERH_USAGE |
| `erh_servicelifeusage` | `float` | `float` |  |  | ERH_SERVICELIFEUSAGE |
| `erh_risklevel` | `string` | `varchar` |  |  | ERH_RISKLEVEL |
| `erh_updatecount` | `float` | `float` |  |  | ERH_UPDATECOUNT |
| `erh_capacityrating` | `float` | `float` |  |  | ERH_CAPACITYRATING |
| `erh_numberoffailures` | `float` | `float` |  |  | ERH_NUMBEROFFAILURES |
| `erh_mtbfdays` | `float` | `float` |  |  | ERH_MTBFDAYS |
| `erh_mtbfrating` | `float` | `float` |  |  | ERH_MTBFRATING |
| `erh_mubf` | `float` | `float` |  |  | ERH_MUBF |
| `erh_mubfrating` | `float` | `float` |  |  | ERH_MUBFRATING |
| `erh_mttrhrs` | `float` | `float` |  |  | ERH_MTTRHRS |
| `erh_mttrrating` | `float` | `float` |  |  | ERH_MTTRRATING |
| `erh_variableresult1` | `float` | `float` |  |  | ERH_VARIABLERESULT1 |
| `erh_variablerating1` | `float` | `float` |  |  | ERH_VARIABLERATING1 |
| `erh_variableresult2` | `float` | `float` |  |  | ERH_VARIABLERESULT2 |
| `erh_variablerating2` | `float` | `float` |  |  | ERH_VARIABLERATING2 |
| `erh_variableresult3` | `float` | `float` |  |  | ERH_VARIABLERESULT3 |
| `erh_variablerating3` | `float` | `float` |  |  | ERH_VARIABLERATING3 |
| `erh_variableresult4` | `float` | `float` |  |  | ERH_VARIABLERESULT4 |
| `erh_variablerating4` | `float` | `float` |  |  | ERH_VARIABLERATING4 |
| `erh_variableresult5` | `float` | `float` |  |  | ERH_VARIABLERESULT5 |
| `erh_variablerating5` | `float` | `float` |  |  | ERH_VARIABLERATING5 |
| `erh_variableresult6` | `float` | `float` |  |  | ERH_VARIABLERESULT6 |
| `erh_variablerating6` | `float` | `float` |  |  | ERH_VARIABLERATING6 |
| `erh_performanceformula` | `string` | `varchar` |  |  | ERH_PERFORMANCEFORMULA |
| `erh_performanceformula_org` | `string` | `varchar` |  |  | ERH_PERFORMANCEFORMULA_ORG |
| `erh_performance` | `float` | `float` |  |  | ERH_PERFORMANCE |
| `erh_conditionrating` | `float` | `float` |  |  | ERH_CONDITIONRATING |
| `erh_capacitycode` | `string` | `varchar` |  |  | ERH_CAPACITYCODE |
| `erh_availablecapacity` | `float` | `float` |  |  | ERH_AVAILABLECAPACITY |
| `erh_desiredcapacity` | `float` | `float` |  |  | ERH_DESIREDCAPACITY |
| `erh_pk` | `string` | `varchar` | `PK` | `unique` | ERH_PK |
| `erh_created` | `timestamp` | `timestamp_ntz` |  |  | ERH_CREATED |
| `erh_createdby` | `string` | `varchar` |  |  | ERH_CREATEDBY |
| `erh_updated` | `timestamp` | `timestamp_ntz` |  |  | ERH_UPDATED |
| `erh_updatedby` | `string` | `varchar` |  |  | ERH_UPDATEDBY |
| `erh_frompoint` | `float` | `float` |  |  | ERH_FROMPOINT |
| `erh_fromrefdesc` | `string` | `varchar` |  |  | ERH_FROMREFDESC |
| `erh_fromgeoref` | `string` | `varchar` |  |  | ERH_FROMGEOREF |
| `erh_topoint` | `float` | `float` |  |  | ERH_TOPOINT |
| `erh_torefdesc` | `string` | `varchar` |  |  | ERH_TOREFDESC |
| `erh_togeoref` | `string` | `varchar` |  |  | ERH_TOGEOREF |
| `erh_udfchar01` | `string` | `varchar` |  |  | ERH_UDFCHAR01 |
| `erh_udfchar02` | `string` | `varchar` |  |  | ERH_UDFCHAR02 |
| `erh_udfchar03` | `string` | `varchar` |  |  | ERH_UDFCHAR03 |
| `erh_udfchar04` | `string` | `varchar` |  |  | ERH_UDFCHAR04 |
| `erh_udfchar05` | `string` | `varchar` |  |  | ERH_UDFCHAR05 |
| `erh_udfchar06` | `string` | `varchar` |  |  | ERH_UDFCHAR06 |
| `erh_udfchar07` | `string` | `varchar` |  |  | ERH_UDFCHAR07 |
| `erh_udfchar08` | `string` | `varchar` |  |  | ERH_UDFCHAR08 |
| `erh_udfchar09` | `string` | `varchar` |  |  | ERH_UDFCHAR09 |
| `erh_udfchar10` | `string` | `varchar` |  |  | ERH_UDFCHAR10 |
| `erh_udfchar11` | `string` | `varchar` |  |  | ERH_UDFCHAR11 |
| `erh_udfchar12` | `string` | `varchar` |  |  | ERH_UDFCHAR12 |
| `erh_udfchar13` | `string` | `varchar` |  |  | ERH_UDFCHAR13 |
| `erh_udfchar14` | `string` | `varchar` |  |  | ERH_UDFCHAR14 |
| `erh_udfchar18` | `string` | `varchar` |  |  | ERH_UDFCHAR18 |
| `erh_udfchar15` | `string` | `varchar` |  |  | ERH_UDFCHAR15 |
| `erh_udfchar16` | `string` | `varchar` |  |  | ERH_UDFCHAR16 |
| `erh_udfchar17` | `string` | `varchar` |  |  | ERH_UDFCHAR17 |
| `erh_udfchar19` | `string` | `varchar` |  |  | ERH_UDFCHAR19 |
| `erh_udfchar20` | `string` | `varchar` |  |  | ERH_UDFCHAR20 |
| `erh_udfchar21` | `string` | `varchar` |  |  | ERH_UDFCHAR21 |
| `erh_udfchar22` | `string` | `varchar` |  |  | ERH_UDFCHAR22 |
| `erh_udfchar23` | `string` | `varchar` |  |  | ERH_UDFCHAR23 |
| `erh_udfchar24` | `string` | `varchar` |  |  | ERH_UDFCHAR24 |
| `erh_udfchar25` | `string` | `varchar` |  |  | ERH_UDFCHAR25 |
| `erh_udfchar26` | `string` | `varchar` |  |  | ERH_UDFCHAR26 |
| `erh_udfchar27` | `string` | `varchar` |  |  | ERH_UDFCHAR27 |
| `erh_udfchar28` | `string` | `varchar` |  |  | ERH_UDFCHAR28 |
| `erh_udfchar29` | `string` | `varchar` |  |  | ERH_UDFCHAR29 |
| `erh_udfchar30` | `string` | `varchar` |  |  | ERH_UDFCHAR30 |
| `erh_udfnum01` | `float` | `float` |  |  | ERH_UDFNUM01 |
| `erh_udfnum02` | `float` | `float` |  |  | ERH_UDFNUM02 |
| `erh_udfnum03` | `float` | `float` |  |  | ERH_UDFNUM03 |
| `erh_udfnum04` | `float` | `float` |  |  | ERH_UDFNUM04 |
| `erh_udfnum05` | `float` | `float` |  |  | ERH_UDFNUM05 |
| `erh_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ERH_UDFDATE01 |
| `erh_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ERH_UDFDATE02 |
| `erh_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ERH_UDFDATE03 |
| `erh_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ERH_UDFDATE04 |
| `erh_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ERH_UDFDATE05 |
| `erh_udfchkbox01` | `string` | `varchar` |  |  | ERH_UDFCHKBOX01 |
| `erh_udfchkbox02` | `string` | `varchar` |  |  | ERH_UDFCHKBOX02 |
| `erh_udfchkbox03` | `string` | `varchar` |  |  | ERH_UDFCHKBOX03 |
| `erh_udfchkbox04` | `string` | `varchar` |  |  | ERH_UDFCHKBOX04 |
| `erh_udfchkbox05` | `string` | `varchar` |  |  | ERH_UDFCHKBOX05 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

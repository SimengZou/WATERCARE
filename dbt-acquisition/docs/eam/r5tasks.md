# r5tasks

eam_R5TASKS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5tasks` |
| **Materialization** | `incremental` |
| **Primary keys** | `tsk_code`, `tsk_revision` |
| **Column count** | 122 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tsk_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TSK_LASTSAVED |
| `tsk_checklistrejectedwostatus` | `string` | `varchar` |  |  | TSK_CHECKLISTREJECTEDWOSTATUS |
| `tsk_preventperformedbysign` | `string` | `varchar` |  |  | TSK_PREVENTPERFORMEDBYSIGN |
| `tsk_preventreviewedbysign` | `string` | `varchar` |  |  | TSK_PREVENTREVIEWEDBYSIGN |
| `tsk_code` | `string` | `varchar` | `PK` |  | TSK_CODE |
| `tsk_desc` | `string` | `varchar` |  |  | TSK_DESC |
| `tsk_class` | `string` | `varchar` |  |  | TSK_CLASS |
| `tsk_revision` | `float` | `float` | `PK` |  | TSK_REVISION |
| `tsk_revrstatus` | `string` | `varchar` |  |  | TSK_REVRSTATUS |
| `tsk_revstatus` | `string` | `varchar` |  |  | TSK_REVSTATUS |
| `tsk_trade` | `string` | `varchar` |  |  | TSK_TRADE |
| `tsk_hours` | `float` | `float` |  |  | TSK_HOURS |
| `tsk_uom` | `string` | `varchar` |  |  | TSK_UOM |
| `tsk_persons` | `float` | `float` |  |  | TSK_PERSONS |
| `tsk_commodity` | `string` | `varchar` |  |  | TSK_COMMODITY |
| `tsk_buyer` | `string` | `varchar` |  |  | TSK_BUYER |
| `tsk_price` | `float` | `float` |  |  | TSK_PRICE |
| `tsk_prefsup` | `string` | `varchar` |  |  | TSK_PREFSUP |
| `tsk_org` | `string` | `varchar` |  |  | TSK_ORG |
| `tsk_class_org` | `string` | `varchar` |  |  | TSK_CLASS_ORG |
| `tsk_prefsup_org` | `string` | `varchar` |  |  | TSK_PREFSUP_ORG |
| `tsk_rpc` | `string` | `varchar` |  |  | TSK_RPC |
| `tsk_wap` | `string` | `varchar` |  |  | TSK_WAP |
| `tsk_tpf` | `string` | `varchar` |  |  | TSK_TPF |
| `tsk_manufact` | `string` | `varchar` |  |  | TSK_MANUFACT |
| `tsk_syslevel` | `string` | `varchar` |  |  | TSK_SYSLEVEL |
| `tsk_asmlevel` | `string` | `varchar` |  |  | TSK_ASMLEVEL |
| `tsk_complevel` | `string` | `varchar` |  |  | TSK_COMPLEVEL |
| `tsk_updatecount` | `float` | `float` |  |  | TSK_UPDATECOUNT |
| `tsk_created` | `timestamp` | `timestamp_ntz` |  |  | TSK_CREATED |
| `tsk_updated` | `timestamp` | `timestamp_ntz` |  |  | TSK_UPDATED |
| `tsk_notused` | `string` | `varchar` |  |  | TSK_NOTUSED |
| `tsk_activechecklist` | `string` | `varchar` |  |  | TSK_ACTIVECHECKLIST |
| `tsk_isolationmethod` | `string` | `varchar` |  |  | TSK_ISOLATIONMETHOD |
| `tsk_checklistperfbyreq` | `string` | `varchar` |  |  | TSK_CHECKLISTPERFBYREQ |
| `tsk_checklistrevbyreq` | `string` | `varchar` |  |  | TSK_CHECKLISTREVBYREQ |
| `tsk_wotype` | `string` | `varchar` |  |  | TSK_WOTYPE |
| `tsk_woclass` | `string` | `varchar` |  |  | TSK_WOCLASS |
| `tsk_woclass_org` | `string` | `varchar` |  |  | TSK_WOCLASS_ORG |
| `tsk_wostatus` | `string` | `varchar` |  |  | TSK_WOSTATUS |
| `tsk_wopriority` | `string` | `varchar` |  |  | TSK_WOPRIORITY |
| `tsk_wodesc` | `string` | `varchar` |  |  | TSK_WODESC |
| `tsk_objtype` | `string` | `varchar` |  |  | TSK_OBJTYPE |
| `tsk_objclass` | `string` | `varchar` |  |  | TSK_OBJCLASS |
| `tsk_objclass_org` | `string` | `varchar` |  |  | TSK_OBJCLASS_ORG |
| `tsk_matlist` | `string` | `varchar` |  |  | TSK_MATLIST |
| `tsk_jobplan` | `string` | `varchar` |  |  | TSK_JOBPLAN |
| `tsk_jobplantype` | `string` | `varchar` |  |  | TSK_JOBPLANTYPE |
| `tsk_multipletrades` | `string` | `varchar` |  |  | TSK_MULTIPLETRADES |
| `tsk_planningrecordsexist` | `string` | `varchar` |  |  | TSK_PLANNINGRECORDSEXIST |
| `tsk_udfchar01` | `string` | `varchar` |  |  | TSK_UDFCHAR01 |
| `tsk_udfchar02` | `string` | `varchar` |  |  | TSK_UDFCHAR02 |
| `tsk_udfchar03` | `string` | `varchar` |  |  | TSK_UDFCHAR03 |
| `tsk_udfchar04` | `string` | `varchar` |  |  | TSK_UDFCHAR04 |
| `tsk_udfchar05` | `string` | `varchar` |  |  | TSK_UDFCHAR05 |
| `tsk_udfchar06` | `string` | `varchar` |  |  | TSK_UDFCHAR06 |
| `tsk_udfchar07` | `string` | `varchar` |  |  | TSK_UDFCHAR07 |
| `tsk_udfchar08` | `string` | `varchar` |  |  | TSK_UDFCHAR08 |
| `tsk_udfchar09` | `string` | `varchar` |  |  | TSK_UDFCHAR09 |
| `tsk_udfchar10` | `string` | `varchar` |  |  | TSK_UDFCHAR10 |
| `tsk_udfchar11` | `string` | `varchar` |  |  | TSK_UDFCHAR11 |
| `tsk_udfchar12` | `string` | `varchar` |  |  | TSK_UDFCHAR12 |
| `tsk_udfchar13` | `string` | `varchar` |  |  | TSK_UDFCHAR13 |
| `tsk_udfchar14` | `string` | `varchar` |  |  | TSK_UDFCHAR14 |
| `tsk_udfchar15` | `string` | `varchar` |  |  | TSK_UDFCHAR15 |
| `tsk_udfchar16` | `string` | `varchar` |  |  | TSK_UDFCHAR16 |
| `tsk_udfchar17` | `string` | `varchar` |  |  | TSK_UDFCHAR17 |
| `tsk_udfchar18` | `string` | `varchar` |  |  | TSK_UDFCHAR18 |
| `tsk_udfchar19` | `string` | `varchar` |  |  | TSK_UDFCHAR19 |
| `tsk_udfchar20` | `string` | `varchar` |  |  | TSK_UDFCHAR20 |
| `tsk_udfchar21` | `string` | `varchar` |  |  | TSK_UDFCHAR21 |
| `tsk_udfchar22` | `string` | `varchar` |  |  | TSK_UDFCHAR22 |
| `tsk_udfchar23` | `string` | `varchar` |  |  | TSK_UDFCHAR23 |
| `tsk_udfchar24` | `string` | `varchar` |  |  | TSK_UDFCHAR24 |
| `tsk_udfchar25` | `string` | `varchar` |  |  | TSK_UDFCHAR25 |
| `tsk_udfchar26` | `string` | `varchar` |  |  | TSK_UDFCHAR26 |
| `tsk_udfchar27` | `string` | `varchar` |  |  | TSK_UDFCHAR27 |
| `tsk_udfchar28` | `string` | `varchar` |  |  | TSK_UDFCHAR28 |
| `tsk_udfchar29` | `string` | `varchar` |  |  | TSK_UDFCHAR29 |
| `tsk_udfchar30` | `string` | `varchar` |  |  | TSK_UDFCHAR30 |
| `tsk_udfnum01` | `float` | `float` |  |  | TSK_UDFNUM01 |
| `tsk_udfnum02` | `float` | `float` |  |  | TSK_UDFNUM02 |
| `tsk_udfnum03` | `float` | `float` |  |  | TSK_UDFNUM03 |
| `tsk_udfnum04` | `float` | `float` |  |  | TSK_UDFNUM04 |
| `tsk_udfnum05` | `float` | `float` |  |  | TSK_UDFNUM05 |
| `tsk_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | TSK_UDFDATE01 |
| `tsk_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | TSK_UDFDATE02 |
| `tsk_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | TSK_UDFDATE03 |
| `tsk_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | TSK_UDFDATE04 |
| `tsk_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | TSK_UDFDATE05 |
| `tsk_udfchkbox01` | `string` | `varchar` |  |  | TSK_UDFCHKBOX01 |
| `tsk_udfchkbox02` | `string` | `varchar` |  |  | TSK_UDFCHKBOX02 |
| `tsk_udfchkbox03` | `string` | `varchar` |  |  | TSK_UDFCHKBOX03 |
| `tsk_udfchkbox04` | `string` | `varchar` |  |  | TSK_UDFCHKBOX04 |
| `tsk_udfchkbox05` | `string` | `varchar` |  |  | TSK_UDFCHKBOX05 |
| `tsk_reusable` | `string` | `varchar` |  |  | TSK_REUSABLE |
| `tsk_type` | `string` | `varchar` |  |  | TSK_TYPE |
| `tsk_planninglevel` | `string` | `varchar` |  |  | TSK_PLANNINGLEVEL |
| `tsk_complocation` | `string` | `varchar` |  |  | TSK_COMPLOCATION |
| `tsk_enableenhancedplanning` | `string` | `varchar` |  |  | TSK_ENABLEENHANCEDPLANNING |
| `tsk_casemanagementchecklist` | `string` | `varchar` |  |  | TSK_CASEMANAGEMENTCHECKLIST |
| `tsk_workspacemoves` | `string` | `varchar` |  |  | TSK_WORKSPACEMOVES |
| `tsk_tagheader` | `string` | `varchar` |  |  | TSK_TAGHEADER |
| `tsk_tagline1` | `string` | `varchar` |  |  | TSK_TAGLINE1 |
| `tsk_tagline2` | `string` | `varchar` |  |  | TSK_TAGLINE2 |
| `tsk_tagline3` | `string` | `varchar` |  |  | TSK_TAGLINE3 |
| `tsk_tagline4` | `string` | `varchar` |  |  | TSK_TAGLINE4 |
| `tsk_defaulttag` | `string` | `varchar` |  |  | TSK_DEFAULTTAG |
| `tsk_nonconformitychecklist` | `string` | `varchar` |  |  | TSK_NONCONFORMITYCHECKLIST |
| `tsk_viewonlyresp` | `string` | `varchar` |  |  | TSK_VIEWONLYRESP |
| `tsk_reviewresp` | `string` | `varchar` |  |  | TSK_REVIEWRESP |
| `tsk_performbyresp` | `string` | `varchar` |  |  | TSK_PERFORMBYRESP |
| `tsk_performby2resp` | `string` | `varchar` |  |  | TSK_PERFORMBY2RESP |
| `tsk_disconnected_chklist` | `string` | `varchar` |  |  | TSK_DISCONNECTED_CHKLIST |
| `tsk_performedbywostatus` | `string` | `varchar` |  |  | TSK_PERFORMEDBYWOSTATUS |
| `tsk_reviewedbywostatus` | `string` | `varchar` |  |  | TSK_REVIEWEDBYWOSTATUS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5wolaborschedparams

eam_R5WOLABORSCHEDPARAMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5wolaborschedparams` |
| **Materialization** | `incremental` |
| **Primary keys** | `wls_code` |
| **Column count** | 89 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wls_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WLS_LASTSAVED |
| `wls_code` | `string` | `varchar` | `PK` | `unique` | WLS_CODE |
| `wls_user` | `string` | `varchar` |  |  | WLS_USER |
| `wls_parameter` | `string` | `varchar` |  |  | WLS_PARAMETER |
| `wls_defparam` | `string` | `varchar` |  |  | WLS_DEFPARAM |
| `wls_sessionid` | `float` | `float` |  |  | WLS_SESSIONID |
| `wls_woschedstartdate` | `timestamp` | `timestamp_ntz` |  |  | WLS_WOSCHEDSTARTDATE |
| `wls_woschedenddate` | `timestamp` | `timestamp_ntz` |  |  | WLS_WOSCHEDENDDATE |
| `wls_created` | `timestamp` | `timestamp_ntz` |  |  | WLS_CREATED |
| `wls_event_org` | `string` | `varchar` |  |  | WLS_EVENT_ORG |
| `wls_workorder` | `string` | `varchar` |  |  | WLS_WORKORDER |
| `wls_womrc` | `string` | `varchar` |  |  | WLS_WOMRC |
| `wls_wopmschedule` | `string` | `varchar` |  |  | WLS_WOPMSCHEDULE |
| `wls_woproject` | `string` | `varchar` |  |  | WLS_WOPROJECT |
| `wls_woprojbud` | `string` | `varchar` |  |  | WLS_WOPROJBUD |
| `wls_woobject` | `string` | `varchar` |  |  | WLS_WOOBJECT |
| `wls_woobjectorg` | `string` | `varchar` |  |  | WLS_WOOBJECTORG |
| `wls_woloc` | `string` | `varchar` |  |  | WLS_WOLOC |
| `wls_wolocorg` | `string` | `varchar` |  |  | WLS_WOLOCORG |
| `wls_wojobtype` | `string` | `varchar` |  |  | WLS_WOJOBTYPE |
| `wls_wopriority` | `string` | `varchar` |  |  | WLS_WOPRIORITY |
| `wls_wostatus` | `string` | `varchar` |  |  | WLS_WOSTATUS |
| `wls_woreportedby` | `string` | `varchar` |  |  | WLS_WOREPORTEDBY |
| `wls_woassignedby` | `string` | `varchar` |  |  | WLS_WOASSIGNEDBY |
| `wls_woassignedto` | `string` | `varchar` |  |  | WLS_WOASSIGNEDTO |
| `wls_woclass` | `string` | `varchar` |  |  | WLS_WOCLASS |
| `wls_woclassorg` | `string` | `varchar` |  |  | WLS_WOCLASSORG |
| `wls_wocriticality` | `string` | `varchar` |  |  | WLS_WOCRITICALITY |
| `wls_woshift` | `string` | `varchar` |  |  | WLS_WOSHIFT |
| `wls_object` | `string` | `varchar` |  |  | WLS_OBJECT |
| `wls_objorg` | `string` | `varchar` |  |  | WLS_OBJORG |
| `wls_objtype` | `string` | `varchar` |  |  | WLS_OBJTYPE |
| `wls_objstatus` | `string` | `varchar` |  |  | WLS_OBJSTATUS |
| `wls_objclass` | `string` | `varchar` |  |  | WLS_OBJCLASS |
| `wls_objclassorg` | `string` | `varchar` |  |  | WLS_OBJCLASSORG |
| `wls_objcategory` | `string` | `varchar` |  |  | WLS_OBJCATEGORY |
| `wls_objcostcode` | `string` | `varchar` |  |  | WLS_OBJCOSTCODE |
| `wls_objmrc` | `string` | `varchar` |  |  | WLS_OBJMRC |
| `wls_objparent` | `string` | `varchar` |  |  | WLS_OBJPARENT |
| `wls_objparentorg` | `string` | `varchar` |  |  | WLS_OBJPARENTORG |
| `wls_objloc` | `string` | `varchar` |  |  | WLS_OBJLOC |
| `wls_objlocorg` | `string` | `varchar` |  |  | WLS_OBJLOCORG |
| `wls_acttrade` | `string` | `varchar` |  |  | WLS_ACTTRADE |
| `wls_acttask` | `string` | `varchar` |  |  | WLS_ACTTASK |
| `wls_actstartdate` | `timestamp` | `timestamp_ntz` |  |  | WLS_ACTSTARTDATE |
| `wls_actenddate` | `timestamp` | `timestamp_ntz` |  |  | WLS_ACTENDDATE |
| `wls_empmrc` | `string` | `varchar` |  |  | WLS_EMPMRC |
| `wls_empclass` | `string` | `varchar` |  |  | WLS_EMPCLASS |
| `wls_empclassorg` | `string` | `varchar` |  |  | WLS_EMPCLASSORG |
| `wls_emptrade` | `string` | `varchar` |  |  | WLS_EMPTRADE |
| `wls_empshift` | `string` | `varchar` |  |  | WLS_EMPSHIFT |
| `wls_empcrew` | `string` | `varchar` |  |  | WLS_EMPCREW |
| `wls_empcreworg` | `string` | `varchar` |  |  | WLS_EMPCREWORG |
| `wls_unschedcolor` | `string` | `varchar` |  |  | WLS_UNSCHEDCOLOR |
| `wls_lightlyschedcolor` | `string` | `varchar` |  |  | WLS_LIGHTLYSCHEDCOLOR |
| `wls_moderatelyschedcolor` | `string` | `varchar` |  |  | WLS_MODERATELYSCHEDCOLOR |
| `wls_fullyschedcolor` | `string` | `varchar` |  |  | WLS_FULLYSCHEDCOLOR |
| `wls_overschedcolor` | `string` | `varchar` |  |  | WLS_OVERSCHEDCOLOR |
| `wls_thresholdpercent` | `float` | `float` |  |  | WLS_THRESHOLDPERCENT |
| `wls_genavailthrough` | `timestamp` | `timestamp_ntz` |  |  | WLS_GENAVAILTHROUGH |
| `wls_scheduling` | `string` | `varchar` |  |  | WLS_SCHEDULING |
| `wls_schedulingstartdate` | `timestamp` | `timestamp_ntz` |  |  | WLS_SCHEDULINGSTARTDATE |
| `wls_updated` | `timestamp` | `timestamp_ntz` |  |  | WLS_UPDATED |
| `wls_listlastrefreshed` | `timestamp` | `timestamp_ntz` |  |  | WLS_LISTLASTREFRESHED |
| `wls_datalastgen` | `timestamp` | `timestamp_ntz` |  |  | WLS_DATALASTGEN |
| `wls_mp` | `string` | `varchar` |  |  | WLS_MP |
| `wls_mp_org` | `string` | `varchar` |  |  | WLS_MP_ORG |
| `wls_mp_seq` | `float` | `float` |  |  | WLS_MP_SEQ |
| `wls_campaign` | `string` | `varchar` |  |  | WLS_CAMPAIGN |
| `wls_campaign_line` | `float` | `float` |  |  | WLS_CAMPAIGN_LINE |
| `wls_updatecount` | `float` | `float` |  |  | WLS_UPDATECOUNT |
| `wls_objincludechildren` | `string` | `varchar` |  |  | WLS_OBJINCLUDECHILDREN |
| `wls_allowactsondiffsession` | `string` | `varchar` |  |  | WLS_ALLOWACTSONDIFFSESSION |
| `wls_skipequipselectionprocess` | `string` | `varchar` |  |  | WLS_SKIPEQUIPSELECTIONPROCESS |
| `wls_maxcalcpriority` | `float` | `float` |  |  | WLS_MAXCALCPRIORITY |
| `wls_operationalstatus` | `string` | `varchar` |  |  | WLS_OPERATIONALSTATUS |
| `wls_shiftscheduling` | `string` | `varchar` |  |  | WLS_SHIFTSCHEDULING |
| `wls_shiftstart` | `timestamp` | `timestamp_ntz` |  |  | WLS_SHIFTSTART |
| `wls_shiftduration` | `float` | `float` |  |  | WLS_SHIFTDURATION |
| `wls_autoselectactivity` | `string` | `varchar` |  |  | WLS_AUTOSELECTACTIVITY |
| `wls_excludeduration` | `string` | `varchar` |  |  | WLS_EXCLUDEDURATION |
| `wls_excludepeoplerequired` | `string` | `varchar` |  |  | WLS_EXCLUDEPEOPLEREQUIRED |
| `wls_consist` | `string` | `varchar` |  |  | WLS_CONSIST |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

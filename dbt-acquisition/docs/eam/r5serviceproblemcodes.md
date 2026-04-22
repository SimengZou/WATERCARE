# r5serviceproblemcodes

eam_R5SERVICEPROBLEMCODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5serviceproblemcodes` |
| **Materialization** | `incremental` |
| **Primary keys** | `spb_code`, `spb_org` |
| **Column count** | 98 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `spb_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SPB_LASTSAVED |
| `spb_code` | `string` | `varchar` | `PK` |  | SPB_CODE |
| `spb_org` | `string` | `varchar` | `PK` |  | SPB_ORG |
| `spb_desc` | `string` | `varchar` |  |  | SPB_DESC |
| `spb_class` | `string` | `varchar` |  |  | SPB_CLASS |
| `spb_class_org` | `string` | `varchar` |  |  | SPB_CLASS_ORG |
| `spb_standwork` | `string` | `varchar` |  |  | SPB_STANDWORK |
| `spb_priority` | `string` | `varchar` |  |  | SPB_PRIORITY |
| `spb_woclass` | `string` | `varchar` |  |  | SPB_WOCLASS |
| `spb_woclass_org` | `string` | `varchar` |  |  | SPB_WOCLASS_ORG |
| `spb_object` | `string` | `varchar` |  |  | SPB_OBJECT |
| `spb_object_org` | `string` | `varchar` |  |  | SPB_OBJECT_ORG |
| `spb_notused` | `string` | `varchar` |  |  | SPB_NOTUSED |
| `spb_jobtype` | `string` | `varchar` |  |  | SPB_JOBTYPE |
| `spb_updatecount` | `float` | `float` |  |  | SPB_UPDATECOUNT |
| `spb_tempfixturnaround` | `float` | `float` |  |  | SPB_TEMPFIXTURNAROUND |
| `spb_tempturnaroundunit` | `string` | `varchar` |  |  | SPB_TEMPTURNAROUNDUNIT |
| `spb_permfixturnaround` | `float` | `float` |  |  | SPB_PERMFIXTURNAROUND |
| `spb_permturnaroundunit` | `string` | `varchar` |  |  | SPB_PERMTURNAROUNDUNIT |
| `spb_penaltyfactor` | `float` | `float` |  |  | SPB_PENALTYFACTOR |
| `spb_udfchar01` | `string` | `varchar` |  |  | SPB_UDFCHAR01 |
| `spb_udfchar02` | `string` | `varchar` |  |  | SPB_UDFCHAR02 |
| `spb_udfchar03` | `string` | `varchar` |  |  | SPB_UDFCHAR03 |
| `spb_udfchar04` | `string` | `varchar` |  |  | SPB_UDFCHAR04 |
| `spb_udfchar05` | `string` | `varchar` |  |  | SPB_UDFCHAR05 |
| `spb_udfchar06` | `string` | `varchar` |  |  | SPB_UDFCHAR06 |
| `spb_udfchar07` | `string` | `varchar` |  |  | SPB_UDFCHAR07 |
| `spb_udfchar08` | `string` | `varchar` |  |  | SPB_UDFCHAR08 |
| `spb_udfchar09` | `string` | `varchar` |  |  | SPB_UDFCHAR09 |
| `spb_udfchar10` | `string` | `varchar` |  |  | SPB_UDFCHAR10 |
| `spb_udfnum01` | `float` | `float` |  |  | SPB_UDFNUM01 |
| `spb_udfnum02` | `float` | `float` |  |  | SPB_UDFNUM02 |
| `spb_udfnum03` | `float` | `float` |  |  | SPB_UDFNUM03 |
| `spb_udfnum04` | `float` | `float` |  |  | SPB_UDFNUM04 |
| `spb_udfnum05` | `float` | `float` |  |  | SPB_UDFNUM05 |
| `spb_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | SPB_UDFDATE01 |
| `spb_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | SPB_UDFDATE02 |
| `spb_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | SPB_UDFDATE03 |
| `spb_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | SPB_UDFDATE04 |
| `spb_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | SPB_UDFDATE05 |
| `spb_udfchkbox01` | `string` | `varchar` |  |  | SPB_UDFCHKBOX01 |
| `spb_udfchkbox02` | `string` | `varchar` |  |  | SPB_UDFCHKBOX02 |
| `spb_udfchkbox03` | `string` | `varchar` |  |  | SPB_UDFCHKBOX03 |
| `spb_udfchkbox04` | `string` | `varchar` |  |  | SPB_UDFCHKBOX04 |
| `spb_udfchkbox05` | `string` | `varchar` |  |  | SPB_UDFCHKBOX05 |
| `spb_eqpusability` | `string` | `varchar` |  |  | SPB_EQPUSABILITY |
| `spb_eqpusability_org` | `string` | `varchar` |  |  | SPB_EQPUSABILITY_ORG |
| `spb_forfollowup` | `string` | `varchar` |  |  | SPB_FORFOLLOWUP |
| `spb_general` | `string` | `varchar` |  |  | SPB_GENERAL |
| `spb_followupservicecode` | `string` | `varchar` |  |  | SPB_FOLLOWUPSERVICECODE |
| `spb_followupservicecode_org` | `string` | `varchar` |  |  | SPB_FOLLOWUPSERVICECODE_ORG |
| `spb_followuptiming` | `string` | `varchar` |  |  | SPB_FOLLOWUPTIMING |
| `spb_wostatus` | `string` | `varchar` |  |  | SPB_WOSTATUS |
| `spb_task` | `string` | `varchar` |  |  | SPB_TASK |
| `spb_jobplan` | `string` | `varchar` |  |  | SPB_JOBPLAN |
| `spb_trade` | `string` | `varchar` |  |  | SPB_TRADE |
| `spb_esthours` | `float` | `float` |  |  | SPB_ESTHOURS |
| `spb_peoplereq` | `float` | `float` |  |  | SPB_PEOPLEREQ |
| `spb_stdresptime` | `float` | `float` |  |  | SPB_STDRESPTIME |
| `spb_stdresptimeunit` | `string` | `varchar` |  |  | SPB_STDRESPTIMEUNIT |
| `spb_casetype` | `string` | `varchar` |  |  | SPB_CASETYPE |
| `spb_caseclass` | `string` | `varchar` |  |  | SPB_CASECLASS |
| `spb_caseclass_org` | `string` | `varchar` |  |  | SPB_CASECLASS_ORG |
| `spb_casepriority` | `string` | `varchar` |  |  | SPB_CASEPRIORITY |
| `spb_costcode` | `string` | `varchar` |  |  | SPB_COSTCODE |
| `spb_regulatory` | `string` | `varchar` |  |  | SPB_REGULATORY |
| `spb_casefollowuprequired` | `string` | `varchar` |  |  | SPB_CASEFOLLOWUPREQUIRED |
| `spb_hazardmaterial` | `string` | `varchar` |  |  | SPB_HAZARDMATERIAL |
| `spb_totalestcost` | `float` | `float` |  |  | SPB_TOTALESTCOST |
| `spb_udfchar11` | `string` | `varchar` |  |  | SPB_UDFCHAR11 |
| `spb_udfchar12` | `string` | `varchar` |  |  | SPB_UDFCHAR12 |
| `spb_autorequestcase` | `string` | `varchar` |  |  | SPB_AUTOREQUESTCASE |
| `spb_udfchar14` | `string` | `varchar` |  |  | SPB_UDFCHAR14 |
| `spb_udfchar15` | `string` | `varchar` |  |  | SPB_UDFCHAR15 |
| `spb_udfchar16` | `string` | `varchar` |  |  | SPB_UDFCHAR16 |
| `spb_udfchar17` | `string` | `varchar` |  |  | SPB_UDFCHAR17 |
| `spb_udfchar18` | `string` | `varchar` |  |  | SPB_UDFCHAR18 |
| `spb_udfchar22` | `string` | `varchar` |  |  | SPB_UDFCHAR22 |
| `spb_udfchar19` | `string` | `varchar` |  |  | SPB_UDFCHAR19 |
| `spb_udfchar20` | `string` | `varchar` |  |  | SPB_UDFCHAR20 |
| `spb_udfchar21` | `string` | `varchar` |  |  | SPB_UDFCHAR21 |
| `spb_udfchar23` | `string` | `varchar` |  |  | SPB_UDFCHAR23 |
| `spb_udfchar24` | `string` | `varchar` |  |  | SPB_UDFCHAR24 |
| `spb_udfchar25` | `string` | `varchar` |  |  | SPB_UDFCHAR25 |
| `spb_udfchar26` | `string` | `varchar` |  |  | SPB_UDFCHAR26 |
| `spb_udfchar27` | `string` | `varchar` |  |  | SPB_UDFCHAR27 |
| `spb_udfchar28` | `string` | `varchar` |  |  | SPB_UDFCHAR28 |
| `spb_udfchar29` | `string` | `varchar` |  |  | SPB_UDFCHAR29 |
| `spb_udfchar30` | `string` | `varchar` |  |  | SPB_UDFCHAR30 |
| `spb_autocreatewo` | `string` | `varchar` |  |  | SPB_AUTOCREATEWO |
| `spb_servicerequestportal` | `string` | `varchar` |  |  | SPB_SERVICEREQUESTPORTAL |
| `spb_udfchar13` | `string` | `varchar` |  |  | SPB_UDFCHAR13 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

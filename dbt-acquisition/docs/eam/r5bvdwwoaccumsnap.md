# r5bvdwwoaccumsnap

eam_R5BVDWWOACCUMSNAP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bvdwwoaccumsnap` |
| **Materialization** | `incremental` |
| **Column count** | 42 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zob_key` | `float` | `float` |  |  | ZOB_KEY |
| `zwo_key` | `float` | `float` |  |  | ZWO_KEY |
| `zdt_key` | `float` | `float` |  |  | ZDT_KEY |
| `zor_key` | `float` | `float` |  |  | ZOR_KEY |
| `zmr_key` | `float` | `float` |  |  | ZMR_KEY |
| `zws_actduration` | `float` | `float` |  |  | ZWS_ACTDURATION |
| `zws_actlaborcostdefcur` | `float` | `float` |  |  | ZWS_ACTLABORCOSTDEFCUR |
| `zws_actlaborcostorgcur` | `float` | `float` |  |  | ZWS_ACTLABORCOSTORGCUR |
| `zws_actlaborhours` | `float` | `float` |  |  | ZWS_ACTLABORHOURS |
| `zws_actmatlcostdefcur` | `float` | `float` |  |  | ZWS_ACTMATLCOSTDEFCUR |
| `zws_actmatlcostorgcur` | `float` | `float` |  |  | ZWS_ACTMATLCOSTORGCUR |
| `zws_actualcompdatekey` | `float` | `float` |  |  | ZWS_ACTUALCOMPDATEKEY |
| `zws_actualstartdatekey` | `float` | `float` |  |  | ZWS_ACTUALSTARTDATEKEY |
| `zws_defcur` | `string` | `varchar` |  |  | ZWS_DEFCUR |
| `zws_downtime` | `float` | `float` |  |  | ZWS_DOWNTIME |
| `zws_downtimecostdefcur` | `float` | `float` |  |  | ZWS_DOWNTIMECOSTDEFCUR |
| `zws_downtimecostorgcur` | `float` | `float` |  |  | ZWS_DOWNTIMECOSTORGCUR |
| `zws_estlaborcostdefcur` | `float` | `float` |  |  | ZWS_ESTLABORCOSTDEFCUR |
| `zws_estlaborcostorgcur` | `float` | `float` |  |  | ZWS_ESTLABORCOSTORGCUR |
| `zws_estlaborhours` | `float` | `float` |  |  | ZWS_ESTLABORHOURS |
| `zws_estmatlcostdefcur` | `float` | `float` |  |  | ZWS_ESTMATLCOSTDEFCUR |
| `zws_estmatlcostorgcur` | `float` | `float` |  |  | ZWS_ESTMATLCOSTORGCUR |
| `zws_orgcur` | `string` | `varchar` |  |  | ZWS_ORGCUR |
| `zws_schedcompdatekey` | `float` | `float` |  |  | ZWS_SCHEDCOMPDATEKEY |
| `zws_schedduration` | `float` | `float` |  |  | ZWS_SCHEDDURATION |
| `zws_scheduledhours` | `float` | `float` |  |  | ZWS_SCHEDULEDHOURS |
| `zws_srdaterequestedkey` | `float` | `float` |  |  | ZWS_SRDATEREQUESTEDKEY |
| `zws_srestcostdefcur` | `float` | `float` |  |  | ZWS_SRESTCOSTDEFCUR |
| `zws_srestcostorgcur` | `float` | `float` |  |  | ZWS_SRESTCOSTORGCUR |
| `zws_srkey` | `float` | `float` |  |  | ZWS_SRKEY |
| `zws_srresponsetime` | `float` | `float` |  |  | ZWS_SRRESPONSETIME |
| `zws_wocreatetoactcomplag` | `float` | `float` |  |  | ZWS_WOCREATETOACTCOMPLAG |
| `zws_wocreatetoactstartlag` | `float` | `float` |  |  | ZWS_WOCREATETOACTSTARTLAG |
| `zws_wocreatetoschedstartlag` | `float` | `float` |  |  | ZWS_WOCREATETOSCHEDSTARTLAG |
| `zws_wodatecreatedkey` | `float` | `float` |  |  | ZWS_WODATECREATEDKEY |
| `zws_wocode` | `string` | `varchar` |  |  | ZWS_WOCODE |
| `zws_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZWS_LASTSAVED |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

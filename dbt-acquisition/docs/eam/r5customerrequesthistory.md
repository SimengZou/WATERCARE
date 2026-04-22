# r5customerrequesthistory

eam_R5CUSTOMERREQUESTHISTORY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5customerrequesthistory` |
| **Materialization** | `incremental` |
| **Primary keys** | `crh_pk` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `crh_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CRH_LASTSAVED |
| `crh_callcentercode` | `string` | `varchar` |  |  | CRH_CALLCENTERCODE |
| `crh_callcenter_org` | `string` | `varchar` |  |  | CRH_CALLCENTER_ORG |
| `crh_reqdate` | `timestamp` | `timestamp_ntz` |  |  | CRH_REQDATE |
| `crh_eventtype` | `string` | `varchar` |  |  | CRH_EVENTTYPE |
| `crh_usercode` | `string` | `varchar` |  |  | CRH_USERCODE |
| `crh_oldvalue` | `string` | `varchar` |  |  | CRH_OLDVALUE |
| `crh_newvalue` | `string` | `varchar` |  |  | CRH_NEWVALUE |
| `crh_updatecount` | `float` | `float` |  |  | CRH_UPDATECOUNT |
| `crh_comment` | `string` | `varchar` |  |  | CRH_COMMENT |
| `crh_event` | `string` | `varchar` |  |  | CRH_EVENT |
| `crh_rentity` | `string` | `varchar` |  |  | CRH_RENTITY |
| `crh_pk` | `string` | `varchar` | `PK` | `unique` | CRH_PK |
| `crh_field` | `string` | `varchar` |  |  | CRH_FIELD |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

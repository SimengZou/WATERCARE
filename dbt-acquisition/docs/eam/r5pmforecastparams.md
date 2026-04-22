# r5pmforecastparams

eam_R5PMFORECASTPARAMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pmforecastparams` |
| **Materialization** | `incremental` |
| **Primary keys** | `pfp_code` |
| **Column count** | 57 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pfp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PFP_LASTSAVED |
| `pfp_parameter` | `string` | `varchar` |  |  | PFP_PARAMETER |
| `pfp_defparam` | `string` | `varchar` |  |  | PFP_DEFPARAM |
| `pfp_sessionid` | `float` | `float` |  |  | PFP_SESSIONID |
| `pfp_startdate` | `timestamp` | `timestamp_ntz` |  |  | PFP_STARTDATE |
| `pfp_enddate` | `timestamp` | `timestamp_ntz` |  |  | PFP_ENDDATE |
| `pfp_objects` | `string` | `varchar` |  |  | PFP_OBJECTS |
| `pfp_obtypes` | `string` | `varchar` |  |  | PFP_OBTYPES |
| `pfp_obclasses` | `string` | `varchar` |  |  | PFP_OBCLASSES |
| `pfp_toplevel` | `string` | `varchar` |  |  | PFP_TOPLEVEL |
| `pfp_toplevel_org` | `string` | `varchar` |  |  | PFP_TOPLEVEL_ORG |
| `pfp_categories` | `string` | `varchar` |  |  | PFP_CATEGORIES |
| `pfp_criticalities` | `string` | `varchar` |  |  | PFP_CRITICALITIES |
| `pfp_pmschedules` | `string` | `varchar` |  |  | PFP_PMSCHEDULES |
| `pfp_pmclasses` | `string` | `varchar` |  |  | PFP_PMCLASSES |
| `pfp_nested` | `string` | `varchar` |  |  | PFP_NESTED |
| `pfp_priorities` | `string` | `varchar` |  |  | PFP_PRIORITIES |
| `pfp_jobtypes` | `string` | `varchar` |  |  | PFP_JOBTYPES |
| `pfp_event_org` | `string` | `varchar` |  |  | PFP_EVENT_ORG |
| `pfp_event_classes` | `string` | `varchar` |  |  | PFP_EVENT_CLASSES |
| `pfp_mrcs` | `string` | `varchar` |  |  | PFP_MRCS |
| `pfp_locations` | `string` | `varchar` |  |  | PFP_LOCATIONS |
| `pfp_persons` | `string` | `varchar` |  |  | PFP_PERSONS |
| `pfp_costcodes` | `string` | `varchar` |  |  | PFP_COSTCODES |
| `pfp_schedgrps` | `string` | `varchar` |  |  | PFP_SCHEDGRPS |
| `pfp_parenttype` | `string` | `varchar` |  |  | PFP_PARENTTYPE |
| `pfp_workhours` | `float` | `float` |  |  | PFP_WORKHOURS |
| `pfp_min_freq` | `float` | `float` |  |  | PFP_MIN_FREQ |
| `pfp_inc_children` | `string` | `varchar` |  |  | PFP_INC_CHILDREN |
| `pfp_workorder_bgcolor` | `string` | `varchar` |  |  | PFP_WORKORDER_BGCOLOR |
| `pfp_actduedt_bgcolor` | `string` | `varchar` |  |  | PFP_ACTDUEDT_BGCOLOR |
| `pfp_forecastpm_bgcolor` | `string` | `varchar` |  |  | PFP_FORECASTPM_BGCOLOR |
| `pfp_weekend_bgcolor` | `string` | `varchar` |  |  | PFP_WEEKEND_BGCOLOR |
| `pfp_lkdpmduedt_txtcolor` | `string` | `varchar` |  |  | PFP_LKDPMDUEDT_TXTCOLOR |
| `pfp_cal_day` | `string` | `varchar` |  |  | PFP_CAL_DAY |
| `pfp_year_dsg` | `string` | `varchar` |  |  | PFP_YEAR_DSG |
| `pfp_qtr_dsg` | `string` | `varchar` |  |  | PFP_QTR_DSG |
| `pfp_month_dsg` | `string` | `varchar` |  |  | PFP_MONTH_DSG |
| `pfp_week_dsg` | `string` | `varchar` |  |  | PFP_WEEK_DSG |
| `pfp_daily_dsg` | `string` | `varchar` |  |  | PFP_DAILY_DSG |
| `pfp_backfilling` | `string` | `varchar` |  |  | PFP_BACKFILLING |
| `pfp_forwardfilling` | `string` | `varchar` |  |  | PFP_FORWARDFILLING |
| `pfp_session_aprv` | `string` | `varchar` |  |  | PFP_SESSION_APRV |
| `pfp_forecasting` | `string` | `varchar` |  |  | PFP_FORECASTING |
| `pfp_created` | `timestamp` | `timestamp_ntz` |  |  | PFP_CREATED |
| `pfp_enablechildequiptab` | `string` | `varchar` |  |  | PFP_ENABLECHILDEQUIPTAB |
| `pfp_screenhsplit` | `float` | `float` |  |  | PFP_SCREENHSPLIT |
| `pfp_pagesize` | `float` | `float` |  |  | PFP_PAGESIZE |
| `pfp_updatecount` | `float` | `float` |  |  | PFP_UPDATECOUNT |
| `pfp_code` | `string` | `varchar` | `PK` | `unique` | PFP_CODE |
| `pfp_user` | `string` | `varchar` |  |  | PFP_USER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

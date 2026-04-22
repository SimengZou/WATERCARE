# r5userlinearpreferences

eam_R5USERLINEARPREFERENCES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userlinearpreferences` |
| **Materialization** | `incremental` |
| **Primary keys** | `ulp_code` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ulp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ULP_LASTSAVED |
| `ulp_lopointsofint` | `string` | `varchar` |  |  | ULP_LOPOINTSOFINT |
| `ulp_lorelatedeq` | `string` | `varchar` |  |  | ULP_LORELATEDEQ |
| `ulp_lorelatedpart` | `string` | `varchar` |  |  | ULP_LORELATEDPART |
| `ulp_lodefaultcolor` | `string` | `varchar` |  |  | ULP_LODEFAULTCOLOR |
| `ulp_lodefaultfilter` | `string` | `varchar` |  |  | ULP_LODEFAULTFILTER |
| `ulp_loincluderelated` | `string` | `varchar` |  |  | ULP_LOINCLUDERELATED |
| `ulp_created` | `timestamp` | `timestamp_ntz` |  |  | ULP_CREATED |
| `ulp_createdby` | `string` | `varchar` |  |  | ULP_CREATEDBY |
| `ulp_updated` | `timestamp` | `timestamp_ntz` |  |  | ULP_UPDATED |
| `ulp_updatedby` | `string` | `varchar` |  |  | ULP_UPDATEDBY |
| `ulp_updatecount` | `float` | `float` |  |  | ULP_UPDATECOUNT |
| `ulp_lorouteandsegment` | `string` | `varchar` |  |  | ULP_LOROUTEANDSEGMENT |
| `ulp_code` | `string` | `varchar` | `PK` | `unique` | ULP_CODE |
| `ulp_desc` | `string` | `varchar` |  |  | ULP_DESC |
| `ulp_default` | `string` | `varchar` |  |  | ULP_DEFAULT |
| `ulp_hidesegments` | `string` | `varchar` |  |  | ULP_HIDESEGMENTS |
| `ulp_hideroues` | `string` | `varchar` |  |  | ULP_HIDEROUES |
| `ulp_groupsegments` | `string` | `varchar` |  |  | ULP_GROUPSEGMENTS |
| `ulp_usercode` | `string` | `varchar` |  |  | ULP_USERCODE |
| `ulp_lolinearref` | `string` | `varchar` |  |  | ULP_LOLINEARREF |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

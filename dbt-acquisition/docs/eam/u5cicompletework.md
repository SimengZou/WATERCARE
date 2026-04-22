# u5cicompletework

eam_U5CICOMPLETEWORK

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5cicompletework` |
| **Materialization** | `incremental` |
| **Primary keys** | `cic_transid` |
| **Column count** | 30 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `cic_resultwonum` | `string` | `varchar` |  |  | CIC_RESULTWONUM |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `cic_type` | `string` | `varchar` |  |  | CIC_TYPE |
| `cic_transid` | `string` | `varchar` | `PK` | `unique` | CIC_TRANSID |
| `cic_reference` | `string` | `varchar` |  |  | CIC_REFERENCE |
| `cic_contractorcode` | `string` | `varchar` |  |  | CIC_CONTRACTORCODE |
| `cic_object` | `string` | `varchar` |  |  | CIC_OBJECT |
| `cic_comment` | `string` | `varchar` |  |  | CIC_COMMENT. Max length: 0 |
| `cic_activitycode` | `string` | `varchar` |  |  | CIC_ACTIVITYCODE |
| `cic_assetcondition` | `string` | `varchar` |  |  | CIC_ASSETCONDITION |
| `cic_crew` | `string` | `varchar` |  |  | CIC_CREW |
| `cic_initiateddate` | `timestamp` | `timestamp_ntz` |  |  | CIC_INITIATEDDATE |
| `cic_startdate` | `timestamp` | `timestamp_ntz` |  |  | CIC_STARTDATE |
| `cic_completeddate` | `timestamp` | `timestamp_ntz` |  |  | CIC_COMPLETEDDATE |
| `cic_resultcode` | `string` | `varchar` |  |  | CIC_RESULTCODE |
| `cic_variationno` | `string` | `varchar` |  |  | CIC_VARIATIONNO |
| `cic_event` | `string` | `varchar` |  |  | CIC_EVENT |
| `cic_completeby` | `string` | `varchar` |  |  | CIC_COMPLETEBY |
| `cic_results` | `string` | `varchar` |  |  | CIC_RESULTS. Max length: 0 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

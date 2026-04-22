# r5requircodes

eam_R5REQUIRCODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5requircodes` |
| **Materialization** | `incremental` |
| **Primary keys** | `rqm_code` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rqm_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RQM_LASTSAVED |
| `rqm_desc` | `string` | `varchar` |  |  | RQM_DESC |
| `rqm_class` | `string` | `varchar` |  |  | RQM_CLASS |
| `rqm_gen` | `string` | `varchar` |  |  | RQM_GEN |
| `rqm_class_org` | `string` | `varchar` |  |  | RQM_CLASS_ORG |
| `rqm_updatecount` | `float` | `float` |  |  | RQM_UPDATECOUNT |
| `rqm_updated` | `timestamp` | `timestamp_ntz` |  |  | RQM_UPDATED |
| `rqm_partfailure` | `string` | `varchar` |  |  | RQM_PARTFAILURE |
| `rqm_notused` | `string` | `varchar` |  |  | RQM_NOTUSED |
| `rqm_enableworkorders` | `string` | `varchar` |  |  | RQM_ENABLEWORKORDERS |
| `rqm_group` | `string` | `varchar` |  |  | RQM_GROUP |
| `rqm_code` | `string` | `varchar` | `PK` | `unique` | RQM_CODE |
| `rqm_created` | `timestamp` | `timestamp_ntz` |  |  | RQM_CREATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

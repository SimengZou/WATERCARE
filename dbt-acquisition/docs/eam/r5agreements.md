# r5agreements

eam_R5AGREEMENTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5agreements` |
| **Materialization** | `incremental` |
| **Primary keys** | `agr_code` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `agr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AGR_LASTSAVED |
| `agr_status` | `string` | `varchar` |  |  | AGR_STATUS |
| `agr_rstatus` | `string` | `varchar` |  |  | AGR_RSTATUS |
| `agr_class` | `string` | `varchar` |  |  | AGR_CLASS |
| `agr_debtor` | `string` | `varchar` |  |  | AGR_DEBTOR |
| `agr_arrangement` | `string` | `varchar` |  |  | AGR_ARRANGEMENT |
| `agr_start` | `timestamp` | `timestamp_ntz` |  |  | AGR_START |
| `agr_end` | `timestamp` | `timestamp_ntz` |  |  | AGR_END |
| `agr_object` | `string` | `varchar` |  |  | AGR_OBJECT |
| `agr_project` | `string` | `varchar` |  |  | AGR_PROJECT |
| `agr_event` | `string` | `varchar` |  |  | AGR_EVENT |
| `agr_org` | `string` | `varchar` |  |  | AGR_ORG |
| `agr_class_org` | `string` | `varchar` |  |  | AGR_CLASS_ORG |
| `agr_object_org` | `string` | `varchar` |  |  | AGR_OBJECT_ORG |
| `agr_updatecount` | `float` | `float` |  |  | AGR_UPDATECOUNT |
| `agr_code` | `string` | `varchar` | `PK` | `unique` | AGR_CODE |
| `agr_desc` | `string` | `varchar` |  |  | AGR_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

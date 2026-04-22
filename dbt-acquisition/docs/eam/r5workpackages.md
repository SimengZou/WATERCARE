# r5workpackages

eam_R5WORKPACKAGES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5workpackages` |
| **Materialization** | `incremental` |
| **Primary keys** | `wpk_code` |
| **Column count** | 32 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wpk_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WPK_LASTSAVED |
| `wpk_org` | `string` | `varchar` |  |  | WPK_ORG |
| `wpk_jobtype` | `string` | `varchar` |  |  | WPK_JOBTYPE |
| `wpk_status` | `string` | `varchar` |  |  | WPK_STATUS |
| `wpk_mrc` | `string` | `varchar` |  |  | WPK_MRC |
| `wpk_class` | `string` | `varchar` |  |  | WPK_CLASS |
| `wpk_class_org` | `string` | `varchar` |  |  | WPK_CLASS_ORG |
| `wpk_jobclass` | `string` | `varchar` |  |  | WPK_JOBCLASS |
| `wpk_jobclass_org` | `string` | `varchar` |  |  | WPK_JOBCLASS_ORG |
| `wpk_ppmtype` | `string` | `varchar` |  |  | WPK_PPMTYPE |
| `wpk_trade` | `string` | `varchar` |  |  | WPK_TRADE |
| `wpk_object` | `string` | `varchar` |  |  | WPK_OBJECT |
| `wpk_object_org` | `string` | `varchar` |  |  | WPK_OBJECT_ORG |
| `wpk_lastevent` | `string` | `varchar` |  |  | WPK_LASTEVENT |
| `wpk_duedate` | `timestamp` | `timestamp_ntz` |  |  | WPK_DUEDATE |
| `wpk_freq` | `float` | `float` |  |  | WPK_FREQ |
| `wpk_duration` | `float` | `float` |  |  | WPK_DURATION |
| `wpk_estworkload` | `float` | `float` |  |  | WPK_ESTWORKLOAD |
| `wpk_persons` | `float` | `float` |  |  | WPK_PERSONS |
| `wpk_changed` | `string` | `varchar` |  |  | WPK_CHANGED |
| `wpk_perioduom` | `string` | `varchar` |  |  | WPK_PERIODUOM |
| `wpk_updatecount` | `float` | `float` |  |  | WPK_UPDATECOUNT |
| `wpk_performonweek` | `string` | `varchar` |  |  | WPK_PERFORMONWEEK |
| `wpk_performonday` | `float` | `float` |  |  | WPK_PERFORMONDAY |
| `wpk_code` | `string` | `varchar` | `PK` | `unique` | WPK_CODE |
| `wpk_desc` | `string` | `varchar` |  |  | WPK_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

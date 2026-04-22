# r5classes

eam_R5CLASSES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5classes` |
| **Materialization** | `incremental` |
| **Primary keys** | `cls_entity`, `cls_code`, `cls_org` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cls_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CLS_LASTSAVED |
| `cls_image` | `string` | `varchar` |  |  | CLS_IMAGE |
| `cls_code` | `string` | `varchar` | `PK` |  | CLS_CODE |
| `cls_desc` | `string` | `varchar` |  |  | CLS_DESC |
| `cls_org` | `string` | `varchar` | `PK` |  | CLS_ORG |
| `cls_rentitycode` | `string` | `varchar` |  |  | CLS_RENTITYCODE |
| `cls_level` | `float` | `float` |  |  | CLS_LEVEL |
| `cls_updatecount` | `float` | `float` |  |  | CLS_UPDATECOUNT |
| `cls_created` | `timestamp` | `timestamp_ntz` |  |  | CLS_CREATED |
| `cls_updated` | `timestamp` | `timestamp_ntz` |  |  | CLS_UPDATED |
| `cls_notused` | `string` | `varchar` |  |  | CLS_NOTUSED |
| `cls_icon` | `string` | `varchar` |  |  | CLS_ICON |
| `cls_iconpath` | `string` | `varchar` |  |  | CLS_ICONPATH |
| `cls_property_definition` | `string` | `varchar` |  |  | CLS_PROPERTY_DEFINITION |
| `cls_mobile_notebook_definition` | `string` | `varchar` |  |  | CLS_MOBILE_NOTEBOOK_DEFINITION |
| `cls_segmentvalue` | `string` | `varchar` |  |  | CLS_SEGMENTVALUE |
| `cls_entity` | `string` | `varchar` | `PK` |  | CLS_ENTITY |
| `cls_rentity` | `string` | `varchar` |  |  | CLS_RENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

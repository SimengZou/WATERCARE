# r5labelformats

eam_R5LABELFORMATS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5labelformats` |
| **Materialization** | `incremental` |
| **Primary keys** | `lbl_code`, `lbl_class`, `lbl_class_org` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lbl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LBL_LASTSAVED |
| `lbl_filename` | `string` | `varchar` |  |  | LBL_FILENAME |
| `lbl_desc` | `string` | `varchar` |  |  | LBL_DESC |
| `lbl_printertype` | `string` | `varchar` |  |  | LBL_PRINTERTYPE |
| `lbl_class` | `string` | `varchar` | `PK` |  | LBL_CLASS |
| `lbl_class_org` | `string` | `varchar` | `PK` |  | LBL_CLASS_ORG |
| `lbl_updatecount` | `float` | `float` |  |  | LBL_UPDATECOUNT |
| `lbl_code` | `string` | `varchar` | `PK` |  | LBL_CODE |
| `lbl_fields` | `string` | `varchar` |  |  | LBL_FIELDS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

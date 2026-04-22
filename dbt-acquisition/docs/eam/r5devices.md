# r5devices

eam_R5DEVICES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5devices` |
| **Materialization** | `incremental` |
| **Primary keys** | `dev_code` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dev_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DEV_LASTSAVED |
| `dev_lastlogindate` | `timestamp` | `timestamp_ntz` |  |  | DEV_LASTLOGINDATE |
| `dev_catflag` | `string` | `varchar` |  |  | DEV_CATFLAG |
| `dev_rtype` | `string` | `varchar` |  |  | DEV_RTYPE |
| `dev_type` | `string` | `varchar` |  |  | DEV_TYPE |
| `dev_category` | `string` | `varchar` |  |  | DEV_CATEGORY |
| `dev_driver` | `string` | `varchar` |  |  | DEV_DRIVER |
| `dev_mode` | `string` | `varchar` |  |  | DEV_MODE |
| `dev_special` | `string` | `varchar` |  |  | DEV_SPECIAL |
| `dev_orientation` | `string` | `varchar` |  |  | DEV_ORIENTATION |
| `dev_destination` | `string` | `varchar` |  |  | DEV_DESTINATION |
| `dev_org` | `string` | `varchar` |  |  | DEV_ORG |
| `dev_updatecount` | `float` | `float` |  |  | DEV_UPDATECOUNT |
| `dev_updated` | `timestamp` | `timestamp_ntz` |  |  | DEV_UPDATED |
| `dev_notused` | `string` | `varchar` |  |  | DEV_NOTUSED |
| `dev_code` | `string` | `varchar` | `PK` | `unique` | DEV_CODE |
| `dev_desc` | `string` | `varchar` |  |  | DEV_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

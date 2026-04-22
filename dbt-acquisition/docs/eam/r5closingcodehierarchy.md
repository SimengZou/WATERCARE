# r5closingcodehierarchy

eam_R5CLOSINGCODEHIERARCHY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5closingcodehierarchy` |
| **Materialization** | `incremental` |
| **Primary keys** | `cch_parentclosingcode`, `cch_parentclosingcodetype`, `cch_childclosingcode`, `cch_childclosingcodetype` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cch_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CCH_LASTSAVED |
| `cch_parentclosingcodetype` | `string` | `varchar` | `PK` |  | CCH_PARENTCLOSINGCODETYPE |
| `cch_childclosingcode` | `string` | `varchar` | `PK` |  | CCH_CHILDCLOSINGCODE |
| `cch_childclosingcodetype` | `string` | `varchar` | `PK` |  | CCH_CHILDCLOSINGCODETYPE |
| `cch_repldept` | `string` | `varchar` |  |  | CCH_REPLDEPT |
| `cch_object_org` | `string` | `varchar` |  |  | CCH_OBJECT_ORG |
| `cch_updatecount` | `float` | `float` |  |  | CCH_UPDATECOUNT |
| `cch_parentclosingcode` | `string` | `varchar` | `PK` |  | CCH_PARENTCLOSINGCODE |
| `cch_object` | `string` | `varchar` |  |  | CCH_OBJECT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

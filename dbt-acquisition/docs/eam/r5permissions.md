# r5permissions

eam_R5PERMISSIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5permissions` |
| **Materialization** | `incremental` |
| **Primary keys** | `prm_function`, `prm_group` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `prm_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PRM_LASTSAVED |
| `prm_select` | `string` | `varchar` |  |  | PRM_SELECT |
| `prm_update` | `string` | `varchar` |  |  | PRM_UPDATE |
| `prm_insert` | `string` | `varchar` |  |  | PRM_INSERT |
| `prm_delete` | `string` | `varchar` |  |  | PRM_DELETE |
| `prm_printer` | `string` | `varchar` |  |  | PRM_PRINTER |
| `prm_screen` | `string` | `varchar` |  |  | PRM_SCREEN |
| `prm_prfile` | `string` | `varchar` |  |  | PRM_PRFILE |
| `prm_local` | `string` | `varchar` |  |  | PRM_LOCAL |
| `prm_defquery` | `string` | `varchar` |  |  | PRM_DEFQUERY |
| `prm_overrule` | `string` | `varchar` |  |  | PRM_OVERRULE |
| `prm_input` | `string` | `varchar` |  |  | PRM_INPUT |
| `prm_updatecount` | `float` | `float` |  |  | PRM_UPDATECOUNT |
| `prm_created` | `timestamp` | `timestamp_ntz` |  |  | PRM_CREATED |
| `prm_updated` | `timestamp` | `timestamp_ntz` |  |  | PRM_UPDATED |
| `prm_securityddspyid` | `float` | `float` |  |  | PRM_SECURITYDDSPYID |
| `prm_function` | `string` | `varchar` | `PK` |  | PRM_FUNCTION |
| `prm_group` | `string` | `varchar` | `PK` |  | PRM_GROUP |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

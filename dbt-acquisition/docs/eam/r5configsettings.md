# r5configsettings

eam_R5CONFIGSETTINGS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5configsettings` |
| **Materialization** | `incremental` |
| **Primary keys** | `cfs_code`, `cfs_group`, `cfs_user`, `cfs_comptype`, `cfs_config`, `cfs_desk` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cfs_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CFS_LASTSAVED |
| `cfs_group` | `string` | `varchar` | `PK` |  | CFS_GROUP |
| `cfs_user` | `string` | `varchar` | `PK` |  | CFS_USER |
| `cfs_comptype` | `string` | `varchar` | `PK` |  | CFS_COMPTYPE |
| `cfs_config` | `float` | `float` | `PK` |  | CFS_CONFIG |
| `cfs_created` | `timestamp` | `timestamp_ntz` |  |  | CFS_CREATED |
| `cfs_updated` | `timestamp` | `timestamp_ntz` |  |  | CFS_UPDATED |
| `cfs_updatecount` | `float` | `float` |  |  | CFS_UPDATECOUNT |
| `cfs_desk` | `string` | `varchar` | `PK` |  | CFS_DESK |
| `cfs_code` | `string` | `varchar` | `PK` |  | CFS_CODE |
| `cfs_xmlconfig` | `string` | `varchar` |  |  | CFS_XMLCONFIG. Max length: 0 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

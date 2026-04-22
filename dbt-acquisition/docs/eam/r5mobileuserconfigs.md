# r5mobileuserconfigs

eam_R5MOBILEUSERCONFIGS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobileuserconfigs` |
| **Materialization** | `incremental` |
| **Primary keys** | `muc_user`, `muc_group`, `muc_code`, `muc_desk` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `muc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MUC_LASTSAVED |
| `muc_group` | `string` | `varchar` | `PK` |  | MUC_GROUP |
| `muc_code` | `string` | `varchar` | `PK` |  | MUC_CODE |
| `muc_desk` | `string` | `varchar` | `PK` |  | MUC_DESK |
| `muc_comptype` | `string` | `varchar` |  |  | MUC_COMPTYPE |
| `muc_created` | `timestamp` | `timestamp_ntz` |  |  | MUC_CREATED |
| `muc_updated` | `timestamp` | `timestamp_ntz` |  |  | MUC_UPDATED |
| `muc_updatecount` | `float` | `float` |  |  | MUC_UPDATECOUNT |
| `muc_user` | `string` | `varchar` | `PK` |  | MUC_USER |
| `muc_rcode` | `string` | `varchar` |  |  | MUC_RCODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5tabpermissions

eam_R5TABPERMISSIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5tabpermissions` |
| **Materialization** | `incremental` |
| **Primary keys** | `trp_function`, `trp_group`, `trp_tab` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `trp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TRP_LASTSAVED |
| `trp_group` | `string` | `varchar` | `PK` |  | TRP_GROUP |
| `trp_visible` | `string` | `varchar` |  |  | TRP_VISIBLE |
| `trp_select` | `string` | `varchar` |  |  | TRP_SELECT |
| `trp_update` | `string` | `varchar` |  |  | TRP_UPDATE |
| `trp_insert` | `string` | `varchar` |  |  | TRP_INSERT |
| `trp_delete` | `string` | `varchar` |  |  | TRP_DELETE |
| `trp_sysrequired` | `string` | `varchar` |  |  | TRP_SYSREQUIRED |
| `trp_updatecount` | `float` | `float` |  |  | TRP_UPDATECOUNT |
| `trp_sequence` | `float` | `float` |  |  | TRP_SEQUENCE |
| `trp_securityddspyid` | `float` | `float` |  |  | TRP_SECURITYDDSPYID |
| `trp_altersave` | `string` | `varchar` |  |  | TRP_ALTERSAVE |
| `trp_updated` | `timestamp` | `timestamp_ntz` |  |  | TRP_UPDATED |
| `trp_function` | `string` | `varchar` | `PK` |  | TRP_FUNCTION |
| `trp_tab` | `string` | `varchar` | `PK` |  | TRP_TAB |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

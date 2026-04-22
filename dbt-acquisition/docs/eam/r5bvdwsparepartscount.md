# r5bvdwsparepartscount

eam_R5BVDWSPAREPARTSCOUNT

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bvdwsparepartscount` |
| **Materialization** | `incremental` |
| **Column count** | 9 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `usr_code` | `string` | `varchar` |  |  | USR_CODE |
| `org_code` | `string` | `varchar` |  |  | ORG_CODE |
| `sto_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | STO_LASTSAVED |
| `spc_count` | `float` | `float` |  |  | SPC_COUNT |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

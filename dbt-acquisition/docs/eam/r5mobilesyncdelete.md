# r5mobilesyncdelete

eam_R5MOBILESYNCDELETE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobilesyncdelete` |
| **Materialization** | `incremental` |
| **Primary keys** | `msd_pk` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `msd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MSD_LASTSAVED |
| `msd_tablename` | `string` | `varchar` |  |  | MSD_TABLENAME |
| `msd_deleted` | `timestamp` | `timestamp_ntz` |  |  | MSD_DELETED |
| `msd_rentity` | `string` | `varchar` |  |  | MSD_RENTITY |
| `msd_org` | `string` | `varchar` |  |  | MSD_ORG |
| `msd_values` | `string` | `varchar` |  |  | MSD_VALUES |
| `msd_updatecount` | `float` | `float` |  |  | MSD_UPDATECOUNT |
| `msd_pk` | `float` | `float` | `PK` | `unique` | MSD_PK |
| `msd_keys` | `string` | `varchar` |  |  | MSD_KEYS |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

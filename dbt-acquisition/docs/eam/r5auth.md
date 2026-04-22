# r5auth

eam_R5AUTH

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5auth` |
| **Materialization** | `incremental` |
| **Primary keys** | `aut_group`, `aut_user`, `aut_entity`, `aut_status`, `aut_statnew` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `aut_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AUT_LASTSAVED |
| `aut_user` | `string` | `varchar` | `PK` |  | AUT_USER |
| `aut_entity` | `string` | `varchar` | `PK` |  | AUT_ENTITY |
| `aut_rentity` | `string` | `varchar` |  |  | AUT_RENTITY |
| `aut_status` | `string` | `varchar` | `PK` |  | AUT_STATUS |
| `aut_type` | `string` | `varchar` |  |  | AUT_TYPE |
| `aut_updatecount` | `float` | `float` |  |  | AUT_UPDATECOUNT |
| `aut_created` | `timestamp` | `timestamp_ntz` |  |  | AUT_CREATED |
| `aut_updated` | `timestamp` | `timestamp_ntz` |  |  | AUT_UPDATED |
| `aut_group` | `string` | `varchar` | `PK` |  | AUT_GROUP |
| `aut_statnew` | `string` | `varchar` | `PK` |  | AUT_STATNEW |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

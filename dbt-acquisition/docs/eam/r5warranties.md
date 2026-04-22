# r5warranties

eam_R5WARRANTIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5warranties` |
| **Materialization** | `incremental` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `war_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WAR_LASTSAVED |
| `war_obtype` | `string` | `varchar` |  |  | WAR_OBTYPE |
| `war_object` | `string` | `varchar` |  |  | WAR_OBJECT |
| `war_object_org` | `string` | `varchar` |  |  | WAR_OBJECT_ORG |
| `war_supplier` | `string` | `varchar` |  |  | WAR_SUPPLIER |
| `war_supplier_org` | `string` | `varchar` |  |  | WAR_SUPPLIER_ORG |
| `war_start` | `timestamp` | `timestamp_ntz` |  |  | WAR_START |
| `war_expiration` | `timestamp` | `timestamp_ntz` |  |  | WAR_EXPIRATION |
| `war_id` | `string` | `varchar` |  |  | WAR_ID |
| `war_desc` | `string` | `varchar` |  |  | WAR_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

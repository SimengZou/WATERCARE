# r5objectupdate

eam_R5OBJECTUPDATE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5objectupdate` |
| **Materialization** | `incremental` |
| **Primary keys** | `oup_pk` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `oup_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OUP_LASTSAVED |
| `oup_oldcode` | `string` | `varchar` |  |  | OUP_OLDCODE |
| `oup_org` | `string` | `varchar` |  |  | OUP_ORG |
| `oup_user` | `string` | `varchar` |  |  | OUP_USER |
| `oup_date` | `timestamp` | `timestamp_ntz` |  |  | OUP_DATE |
| `oup_pk` | `float` | `float` | `PK` | `unique` | OUP_PK |
| `oup_newcode` | `string` | `varchar` |  |  | OUP_NEWCODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# u5contprojactivities

eam_U5CONTPROJACTIVITIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5contprojactivities` |
| **Materialization** | `incremental` |
| **Primary keys** | `csa_contract`, `csa_org`, `csa_scheduleitem` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `csa_org` | `string` | `varchar` | `PK` |  | CSA_ORG |
| `csa_scheduleitem` | `string` | `varchar` | `PK` |  | CSA_SCHEDULEITEM |
| `csa_project` | `string` | `varchar` |  |  | CSA_PROJECT |
| `csa_activity` | `string` | `varchar` |  |  | CSA_ACTIVITY |
| `csa_contractcode` | `string` | `varchar` |  |  | CSA_CONTRACTCODE |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `csa_supplier` | `string` | `varchar` |  |  | CSA_SUPPLIER |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `csa_contract` | `string` | `varchar` | `PK` |  | CSA_CONTRACT |
| `csa_contractor` | `string` | `varchar` |  |  | CSA_CONTRACTOR |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

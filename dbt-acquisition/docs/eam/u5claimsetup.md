# u5claimsetup

eam_U5CLAIMSETUP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5claimsetup` |
| **Materialization** | `incremental` |
| **Primary keys** | `cls_code`, `cls_costitem`, `cls_contract` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `cls_costitem` | `string` | `varchar` | `PK` |  | CLS_COSTITEM |
| `cls_contract` | `string` | `varchar` | `PK` |  | CLS_CONTRACT |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `cls_projactivity` | `string` | `varchar` |  |  | CLS_PROJACTIVITY |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `cls_stagingcostcenter` | `string` | `varchar` |  |  | CLS_STAGINGCOSTCENTER |
| `cls_supplier` | `string` | `varchar` |  |  | CLS_SUPPLIER |
| `cls_project` | `string` | `varchar` |  |  | CLS_PROJECT |
| `cls_code` | `string` | `varchar` | `PK` |  | CLS_CODE |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

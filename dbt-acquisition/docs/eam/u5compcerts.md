# u5compcerts

eam_U5COMPCERTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5compcerts` |
| **Materialization** | `incremental` |
| **Primary keys** | `cmp_object`, `cmp_object_org`, `cmp_pk` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `cmp_pk` | `string` | `varchar` | `PK` |  | CMP_PK |
| `cmp_type` | `string` | `varchar` |  |  | CMP_TYPE |
| `cmp_issuer` | `string` | `varchar` |  |  | CMP_ISSUER |
| `cmp_certifier` | `string` | `varchar` |  |  | CMP_CERTIFIER |
| `cmp_certnum` | `string` | `varchar` |  |  | CMP_CERTNUM |
| `cmp_issuedate` | `timestamp` | `timestamp_ntz` |  |  | CMP_ISSUEDATE |
| `cmp_expdate` | `timestamp` | `timestamp_ntz` |  |  | CMP_EXPDATE |
| `cmp_scope` | `string` | `varchar` |  |  | CMP_SCOPE. Max length: 0 |
| `cmp_status` | `string` | `varchar` |  |  | CMP_STATUS |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `cmp_object` | `string` | `varchar` | `PK` |  | CMP_OBJECT |
| `cmp_object_org` | `string` | `varchar` | `PK` |  | CMP_OBJECT_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

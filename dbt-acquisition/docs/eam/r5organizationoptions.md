# r5organizationoptions

eam_R5ORGANIZATIONOPTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5organizationoptions` |
| **Materialization** | `incremental` |
| **Primary keys** | `opa_code`, `opa_org`, `opa_system` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `opa_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OPA_LASTSAVED |
| `opa_org` | `string` | `varchar` | `PK` |  | OPA_ORG |
| `opa_system` | `string` | `varchar` | `PK` |  | OPA_SYSTEM |
| `opa_desc` | `string` | `varchar` |  |  | OPA_DESC |
| `opa_comment` | `string` | `varchar` |  |  | OPA_COMMENT |
| `opa_module` | `string` | `varchar` |  |  | OPA_MODULE |
| `opa_type` | `string` | `varchar` |  |  | OPA_TYPE |
| `opa_validvalues` | `string` | `varchar` |  |  | OPA_VALIDVALUES |
| `opa_updatecount` | `float` | `float` |  |  | OPA_UPDATECOUNT |
| `opa_code` | `string` | `varchar` | `PK` |  | OPA_CODE |
| `opa_fixed` | `string` | `varchar` |  |  | OPA_FIXED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

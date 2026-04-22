# u5address

eam_U5ADDRESS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5address` |
| **Materialization** | `incremental` |
| **Primary keys** | `adr_key` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `adr_org` | `string` | `varchar` |  |  | ADR_ORG |
| `adr_houseno` | `string` | `varchar` |  |  | ADR_HOUSENO |
| `adr_street` | `string` | `varchar` |  |  | ADR_STREET |
| `adr_suburb` | `string` | `varchar` |  |  | ADR_SUBURB |
| `adr_state` | `string` | `varchar` |  |  | ADR_STATE |
| `adr_postalcode` | `string` | `varchar` |  |  | ADR_POSTALCODE |
| `adr_streettype` | `string` | `varchar` |  |  | ADR_STREETTYPE |
| `adr_subdivision` | `string` | `varchar` |  |  | ADR_SUBDIVISION |
| `adr_servicearea` | `string` | `varchar` |  |  | ADR_SERVICEAREA |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `adr_key` | `string` | `varchar` | `PK` | `unique` | ADR_KEY |
| `adr_flat` | `string` | `varchar` |  |  | ADR_FLAT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# u5myfacilities

eam_U5MYFACILITIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5myfacilities` |
| **Materialization** | `incremental` |
| **Primary keys** | `mfa_user`, `mfa_faccode` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `mfa_faccode` | `string` | `varchar` | `PK` |  | MFA_FACCODE |
| `mfa_facdesc` | `string` | `varchar` |  |  | MFA_FACDESC |
| `mfa_mrc` | `string` | `varchar` |  |  | MFA_MRC |
| `mfa_mrcdesc` | `string` | `varchar` |  |  | MFA_MRCDESC |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `mfa_user` | `string` | `varchar` | `PK` |  | MFA_USER |
| `mfa_facorg` | `string` | `varchar` |  |  | MFA_FACORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

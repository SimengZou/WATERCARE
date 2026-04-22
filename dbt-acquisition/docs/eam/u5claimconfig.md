# u5claimconfig

eam_U5CLAIMCONFIG

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5claimconfig` |
| **Materialization** | `incremental` |
| **Primary keys** | `clc_org`, `clc_contractor` |
| **Column count** | 33 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `clc_childworateapp` | `string` | `varchar` |  |  | CLC_CHILDWORATEAPP |
| `clc_costitem` | `string` | `varchar` |  |  | CLC_COSTITEM |
| `clc_lncompany` | `string` | `varchar` |  |  | CLC_LNCOMPANY |
| `clc_stgcostcode` | `string` | `varchar` |  |  | CLC_STGCOSTCODE |
| `clc_projectnumber` | `string` | `varchar` |  |  | CLC_PROJECTNUMBER |
| `clc_projectactivity` | `string` | `varchar` |  |  | CLC_PROJECTACTIVITY |
| `clc_supplier` | `string` | `varchar` |  |  | CLC_SUPPLIER |
| `clc_store` | `string` | `varchar` |  |  | CLC_STORE |
| `clc_creator` | `string` | `varchar` |  |  | CLC_CREATOR |
| `clc_requestor` | `string` | `varchar` |  |  | CLC_REQUESTOR |
| `clc_requestor2` | `string` | `varchar` |  |  | CLC_REQUESTOR2 |
| `clc_purchaseoffice` | `string` | `varchar` |  |  | CLC_PURCHASEOFFICE |
| `clc_contordertype` | `string` | `varchar` |  |  | CLC_CONTORDERTYPE |
| `clc_noncontordertype` | `string` | `varchar` |  |  | CLC_NONCONTORDERTYPE |
| `clc_orderseries` | `string` | `varchar` |  |  | CLC_ORDERSERIES |
| `clc_excludewofromln` | `string` | `varchar` |  |  | CLC_EXCLUDEWOFROMLN |
| `clc_excludepofromln` | `string` | `varchar` |  |  | CLC_EXCLUDEPOFROMLN |
| `clc_splitintercompanycosts` | `string` | `varchar` |  |  | CLC_SPLITINTERCOMPANYCOSTS |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `clc_pmworateapp` | `string` | `varchar` |  |  | CLC_PMWORATEAPP |
| `clc_org` | `string` | `varchar` | `PK` |  | CLC_ORG |
| `clc_contractor` | `string` | `varchar` | `PK` |  | CLC_CONTRACTOR |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

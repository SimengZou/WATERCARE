# txptc903

LN txptc903 Change Request Header table, Generated 2026-04-10T19:42:43Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_txptc903` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `creq` |
| **Column count** | 32 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `creq` | `string` | `varchar` | `PK` | `not_null` | (required) Change Request. Max length: 9 |
| `proj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `prma` | `string` | `varchar` |  |  | Project manager. Max length: 9 |
| `prph` | `string` | `varchar` |  |  | Project Phase. Max length: 3 |
| `pram` | `float` | `float` |  |  | Change Amount |
| `fapp` | `string` | `varchar` |  |  | First Approver. Max length: 9 |
| `requ` | `string` | `varchar` |  |  | Requestor. Max length: 9 |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: txstat.created, txstat.sub.appr, txstat.approved, txstat.processed, txstat.rejected, txstat.recall |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Submission Date |
| `suby` | `string` | `varchar` |  |  | Submitted by. Max length: 16 |
| `arda` | `timestamp` | `timestamp_ntz` |  |  | Approval / Rejection Date |
| `arby` | `string` | `varchar` |  |  | Approved / Rejected by. Max length: 16 |
| `fapr` | `string` | `varchar` |  |  | Finance. Max length: 16 |
| `fadt` | `timestamp` | `timestamp_ntz` |  |  | Finance Approved Date |
| `pmoa` | `string` | `varchar` |  |  | Project Management Office. Max length: 16 |
| `padt` | `timestamp` | `timestamp_ntz` |  |  | PMO Approved Date |
| `vamt` | `float` | `float` |  |  | Variation Amount |
| `proj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `prph_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm085 Phases |
| `fapp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `deleted` | `boolean` | `boolean` |  | `not_null` | (required) Whether record is deleted |
| `username` | `string` | `varchar` |  | `not_null` | (required) User responsible for record action. Max length: 8 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Time the action occurred |
| `sequencenumber` | `integer` | `int` |  | `not_null` | (required) Sequence number of the action |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

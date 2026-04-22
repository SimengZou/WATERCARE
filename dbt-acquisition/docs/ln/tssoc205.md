# tssoc205

LN tssoc205 Service Engineer Assignments table, Generated 2026-04-10T19:42:38Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tssoc205` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orgn`, `orno`, `line` |
| **Column count** | 55 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orgn` | `integer` | `int` | `PK` | `not_null` | (required) Origin. Allowed values: 5, 10 |
| `orgn_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tstdm.corg.service.order, tstdm.corg.work.order |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `line` | `integer` | `int` | `PK` | `not_null` | (required) Line Number |
| `emno` | `string` | `varchar` |  |  | Service Engineer. Max length: 9 |
| `acln` | `integer` | `int` |  |  | Activity Line Number |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 5, 10, 15, 20, 25 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tssoc.esta.assigned, tssoc.esta.accepted, tssoc.esta.started, tssoc.esta.completed, tssoc.esta.rejected |
| `dltm` | `timestamp` | `timestamp_ntz` |  |  | Downloaded Time |
| `actm` | `timestamp` | `timestamp_ntz` |  |  | Accept Time |
| `rjtm` | `timestamp` | `timestamp_ntz` |  |  | Reject Time |
| `rejr` | `string` | `varchar` |  |  | Rejection Reason. Max length: 6 |
| `pstm` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Time |
| `pftm` | `timestamp` | `timestamp_ntz` |  |  | Planned Finish Time |
| `ptst` | `timestamp` | `timestamp_ntz` |  |  | Planned Travel Start Time |
| `ptft` | `timestamp` | `timestamp_ntz` |  |  | Planned Travel Finish Time |
| `astm` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Time |
| `aftm` | `timestamp` | `timestamp_ntz` |  |  | Actual Finish Time |
| `atst` | `timestamp` | `timestamp_ntz` |  |  | Actual Travel Start Time |
| `atft` | `timestamp` | `timestamp_ntz` |  |  | Actual Travel Finish Time |
| `jctm` | `timestamp` | `timestamp_ntz` |  |  | Job Sheet Completion Time |
| `ltct` | `timestamp` | `timestamp_ntz` |  |  | Last Changed Time |
| `esdt` | `timestamp` | `timestamp_ntz` |  |  | Escalation Date/Time |
| `acdu` | `float` | `float` |  |  | Activity Duration |
| `trdu` | `float` | `float` |  |  | Travel Duration |
| `trdi` | `float` | `float` |  |  | Travel Distance |
| `uecp` | `integer` | `int` |  |  | Use Engineer Calendar for Planning. Allowed values: 1, 2 |
| `uecp_kw` | `string` | `varchar` |  |  | Use Engineer Calendar for Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pged` | `integer` | `int` |  |  | Pager / Phone Enabled. Allowed values: 1, 2 |
| `pged_kw` | `string` | `varchar` |  |  | Pager / Phone Enabled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pgrd` | `integer` | `int` |  |  | Page Required. Allowed values: 1, 2 |
| `pgrd_kw` | `string` | `varchar` |  |  | Page Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `actp` | `integer` | `int` |  |  | Activate Paging. Allowed values: 1, 2 |
| `actp_kw` | `string` | `varchar` |  |  | Activate Paging (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pgit` | `integer` | `int` |  |  | Paging Interval |
| `pgdt` | `timestamp` | `timestamp_ntz` |  |  | Last Page Sent |
| `pgcn` | `integer` | `int` |  |  | Paging Counter |
| `txta` | `integer` | `int` |  |  | Assignment Text |
| `cmbb_orno_acln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tssoc210 Service Order Activities |
| `cmbc_orno_acln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tswcs210 Work Order Activities |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `rejr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `trdi_buln` | `float` | `float` |  |  | CC: Travel Distance in Base Unit Length |
| `trdu_butm` | `float` | `float` |  |  | CC: Travel duration in Base Unit Time |
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

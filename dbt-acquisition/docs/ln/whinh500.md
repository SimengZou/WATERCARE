# whinh500

LN whinh500 Cycle Counting Orders table, Generated 2026-04-10T19:42:51Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh500` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `cntn` |
| **Column count** | 37 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `cntn` | `integer` | `int` | `PK` | `not_null` | (required) Count Number |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `rcyn` | `integer` | `int` |  |  | Reconciliation. Allowed values: 1, 2 |
| `rcyn_kw` | `string` | `varchar` |  |  | Reconciliation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `prnt` | `integer` | `int` |  |  | Printed. Allowed values: 1, 2 |
| `prnt_kw` | `string` | `varchar` |  |  | Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prcc` | `integer` | `int` |  |  | Process Count. Allowed values: 1, 2 |
| `prcc_kw` | `string` | `varchar` |  |  | Process Count (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccst` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30 |
| `ccst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: whinh.ccst.open, whinh.ccst.modified, whinh.ccst.processed |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference Number - base language. Max length: 30 |
| `redt` | `timestamp` | `timestamp_ntz` |  |  | Reference Date |
| `txta` | `integer` | `int` |  |  | Header Text |
| `cdf_adat` | `timestamp` | `timestamp_ntz` |  |  | Approved Date |
| `cdf_ausr` | `string` | `varchar` |  |  | Approved By. Max length: 16 |
| `cdf_rdte` | `timestamp` | `timestamp_ntz` |  |  | Reviewed Date |
| `cdf_rfin` | `integer` | `int` |  |  | Reviewed by Finance. Allowed values: 1, 2 |
| `cdf_rfin_kw` | `string` | `varchar` |  |  | Reviewed by Finance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdf_rusr` | `string` | `varchar` |  |  | Reviewed by User. Max length: 16 |
| `cdf_wfst` | `integer` | `int` |  |  | Workflow Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_wfst_kw` | `string` | `varchar` |  |  | Workflow Status (keyword). Allowed values: whcdf_lst003.not.applicable, whcdf_lst003.approved, whcdf_lst003.rejected, whcdf_lst003.pending, whcdf_lst003.recall, whcdf_lst003.appr.received |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

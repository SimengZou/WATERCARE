# tffam830

LN tffam830 Depreciation Transaction table, Generated 2026-04-10T19:41:36Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam830` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `tkey` |
| **Column count** | 50 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `tkey` | `integer` | `int` | `PK` | `not_null` | (required) Transaction Key |
| `abdf` | `integer` | `int` |  |  | Book Distr. Depr.. Allowed values: 0, 1, 2 |
| `abdf_kw` | `string` | `varchar` |  |  | Book Distr. Depr. (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `cost_1` | `float` | `float` |  |  | Current Cost |
| `cost_2` | `float` | `float` |  |  | Current Cost |
| `cost_3` | `float` | `float` |  |  | Current Cost |
| `curd` | `date` | `date` |  |  | Recovery Date |
| `depr_1` | `float` | `float` |  |  | Depreciation Expense |
| `depr_2` | `float` | `float` |  |  | Depreciation Expense |
| `depr_3` | `float` | `float` |  |  | Depreciation Expense |
| `dtty` | `integer` | `int` |  |  | Depreciation Transaction Type. Allowed values: 0, 1, 2, 3, 4, 5, 6 |
| `dtty_kw` | `string` | `varchar` |  |  | Depreciation Transaction Type (keyword). Allowed values: , tffam.dtty.normal, tffam.dtty.add.post, tffam.dtty.econ.recap, tffam.dtty.reva.depr, tffam.dtty.econ.reva.depr, tffam.dtty.retr.reva.depr |
| `jobn` | `integer` | `int` |  |  | Job Number |
| `last` | `date` | `date` |  |  | Last Depreciation Date |
| `ltdd_1` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_2` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_3` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdu` | `integer` | `int` |  |  | LTD Units |
| `meth` | `string` | `varchar` |  |  | Depreciation Code. Max length: 20 |
| `prop` | `string` | `varchar` |  |  | Property Type. Max length: 10 |
| `susp` | `integer` | `int` |  |  | Suspended. Allowed values: 1, 2 |
| `susp_kw` | `string` | `varchar` |  |  | Suspended (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `unit` | `integer` | `int` |  |  | Units Used |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `ytdd_1` | `float` | `float` |  |  | YTD Depreciation |
| `ytdd_2` | `float` | `float` |  |  | YTD Depreciation |
| `ytdd_3` | `float` | `float` |  |  | YTD Depreciation |
| `adpc` | `float` | `float` |  |  | Accelerated Depreciation Percentage |
| `rcst_1` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_2` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_3` | `float` | `float` |  |  | Revaluation Cost |
| `ltdr_1` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_2` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_3` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ytdr_1` | `float` | `float` |  |  | YTD Revaluation Depreciation |
| `ytdr_2` | `float` | `float` |  |  | YTD Revaluation Depreciation |
| `ytdr_3` | `float` | `float` |  |  | YTD Revaluation Depreciation |
| `tkey_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam800 Transaction |
| `meth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `prop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam780 Property Type |
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

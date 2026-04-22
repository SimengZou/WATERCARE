# tffam820

LN tffam820 Adjustment Transaction table, Generated 2026-04-10T19:41:36Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam820` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `tkey` |
| **Column count** | 71 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `tkey` | `integer` | `int` | `PK` | `not_null` | (required) Transaction Key |
| `acmc_1` | `float` | `float` |  |  | Accumulated Maintenance Cost |
| `acmc_2` | `float` | `float` |  |  | Accumulated Maintenance Cost |
| `acmc_3` | `float` | `float` |  |  | Accumulated Maintenance Cost |
| `acmz` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `acmz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `apam_1` | `float` | `float` |  |  | Additional Posting Amount |
| `apam_2` | `float` | `float` |  |  | Additional Posting Amount |
| `apam_3` | `float` | `float` |  |  | Additional Posting Amount |
| `apaz` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `apaz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `capi_1` | `float` | `float` |  |  | Capitalized Interest |
| `capi_2` | `float` | `float` |  |  | Capitalized Interest |
| `capi_3` | `float` | `float` |  |  | Capitalized Interest |
| `capz` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `capz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `cost_1` | `float` | `float` |  |  | Asset Curr. Cost |
| `cost_2` | `float` | `float` |  |  | Asset Curr. Cost |
| `cost_3` | `float` | `float` |  |  | Asset Curr. Cost |
| `cosz` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `cosz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `curr` | `string` | `varchar` |  |  | Transaction Currency. Max length: 3 |
| `itca_1` | `float` | `float` |  |  | Current ITC Amount |
| `itca_2` | `float` | `float` |  |  | Current ITC Amount |
| `itca_3` | `float` | `float` |  |  | Current ITC Amount |
| `itcz` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `itcz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `life` | `integer` | `int` |  |  | Asset Life |
| `lifz` | `integer` | `int` |  |  | Operator. Allowed values: 0, 1, 2, 3, 4, 5, 8, 9 |
| `lifz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: , tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `ltdd_1` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_2` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_3` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdz` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `ltdz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `reas` | `string` | `varchar` |  |  | Reason. Max length: 10 |
| `revl` | `integer` | `int` |  |  | Revaluation Adjustment. Allowed values: 0, 1, 2 |
| `revl_kw` | `string` | `varchar` |  |  | Revaluation Adjustment (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `s179_1` | `float` | `float` |  |  | Section 179 Value |
| `s179_2` | `float` | `float` |  |  | Section 179 Value |
| `s179_3` | `float` | `float` |  |  | Section 179 Value |
| `s17z` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `s17z_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `salv_1` | `float` | `float` |  |  | Salvage Value |
| `salv_2` | `float` | `float` |  |  | Salvage Value |
| `salv_3` | `float` | `float` |  |  | Salvage Value |
| `salz` | `integer` | `int` |  |  | Operator. Allowed values: 1, 2, 3, 4, 5, 8, 9 |
| `salz_kw` | `string` | `varchar` |  |  | Operator (keyword). Allowed values: tffam.oper.add, tffam.oper.subt, tffam.oper.mult, tffam.oper.divi, tffam.oper.repl, tffam.oper.no.change, tffam.oper.full.depr |
| `traf` | `integer` | `int` |  |  | Tran Flag. Allowed values: 0, 1, 2 |
| `traf_kw` | `string` | `varchar` |  |  | Tran Flag (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `tkey_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam800 Transaction |
| `reas_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam650 Reasons |
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

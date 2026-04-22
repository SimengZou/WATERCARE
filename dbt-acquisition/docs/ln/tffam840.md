# tffam840

LN tffam840 Disposal Transaction table, Generated 2026-04-10T19:41:36Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam840` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `tkey` |
| **Column count** | 54 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `tkey` | `integer` | `int` | `PK` | `not_null` | (required) Transaction Key |
| `adrt` | `integer` | `int` |  |  | ADR Disposal Type. Allowed values: 1, 2, 3 |
| `adrt_kw` | `string` | `varchar` |  |  | ADR Disposal Type (keyword). Allowed values: tffam.adrt.none, tffam.adrt.ordi, tffam.adrt.extr |
| `cost_1` | `float` | `float` |  |  | Current Cost |
| `cost_2` | `float` | `float` |  |  | Current Cost |
| `cost_3` | `float` | `float` |  |  | Current Cost |
| `curr` | `string` | `varchar` |  |  | Transaction Currency. Max length: 3 |
| `dqty` | `float` | `float` |  |  | Quantity |
| `jobn` | `integer` | `int` |  |  | Job Number |
| `life` | `integer` | `int` |  |  | Asset Life |
| `ltdd_1` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_2` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_3` | `float` | `float` |  |  | Accumulated Depreciation |
| `proc_1` | `float` | `float` |  |  | Proceeds |
| `proc_2` | `float` | `float` |  |  | Proceeds |
| `proc_3` | `float` | `float` |  |  | Proceeds |
| `prod` | `integer` | `int` |  |  | Period |
| `prot` | `float` | `float` |  |  | Proceeds |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `reas` | `string` | `varchar` |  |  | Reason. Max length: 10 |
| `refn` | `string` | `varchar` |  |  | Reference Number. Max length: 2 |
| `salv_1` | `float` | `float` |  |  | Salvage Value |
| `salv_2` | `float` | `float` |  |  | Salvage Value |
| `salv_3` | `float` | `float` |  |  | Salvage Value |
| `tagn__en_us` | `string` | `varchar` |  | `not_null` | (required) Tag Number - base language. Max length: 15 |
| `type` | `integer` | `int` |  |  | Disposal Type. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `type_kw` | `string` | `varchar` |  |  | Disposal Type (keyword). Allowed values: tffam.dtyp.sale, tffam.dtyp.dona, tffam.dtyp.aban, tffam.dtyp.casu.loss, tffam.dtyp.thef, tffam.dtyp.trad.in, tffam.dtyp.wrh.transfer |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `year` | `integer` | `int` |  |  | Year |
| `rcst_1` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_2` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_3` | `float` | `float` |  |  | Revaluation Cost |
| `ltdr_1` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_2` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_3` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
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

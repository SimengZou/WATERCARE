# tffam810

LN tffam810 Acquisition Transaction table, Generated 2026-04-10T19:41:35Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam810` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `tkey` |
| **Column count** | 53 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `tkey` | `integer` | `int` | `PK` | `not_null` | (required) Transaction Key |
| `acmc_1` | `float` | `float` |  |  | Accum. Maint. Cost |
| `acmc_2` | `float` | `float` |  |  | Accum. Maint. Cost |
| `acmc_3` | `float` | `float` |  |  | Accum. Maint. Cost |
| `acmt` | `float` | `float` |  |  | Accum. Maint. Cost |
| `agrp` | `string` | `varchar` |  |  | Group. Max length: 10 |
| `amnt_1` | `float` | `float` |  |  | Transaction Amount |
| `amnt_2` | `float` | `float` |  |  | Transaction Amount |
| `amnt_3` | `float` | `float` |  |  | Transaction Amount |
| `amtt` | `float` | `float` |  |  | Transaction Amount |
| `ctgy` | `string` | `varchar` |  |  | Category. Max length: 10 |
| `curr` | `string` | `varchar` |  |  | Transaction Currency. Max length: 3 |
| `jobn` | `integer` | `int` |  |  | Job Number |
| `ltdd_1` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_2` | `float` | `float` |  |  | Accumulated Depreciation |
| `ltdd_3` | `float` | `float` |  |  | Accumulated Depreciation |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `reas` | `string` | `varchar` |  |  | Reason. Max length: 10 |
| `sctg` | `string` | `varchar` |  |  | Default Subcategory. Max length: 10 |
| `tagn__en_us` | `string` | `varchar` |  | `not_null` | (required) Tag Number - base language. Max length: 15 |
| `trid` | `integer` | `int` |  |  | Transfer Id |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `xaex` | `string` | `varchar` |  |  | Asset Extension. Max length: 15 |
| `xanb` | `string` | `varchar` |  |  | Asset Number. Max length: 15 |
| `xcom` | `integer` | `int` |  |  | Company |
| `rcst_1` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_2` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_3` | `float` | `float` |  |  | Revaluation Cost |
| `ltdr_1` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_2` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `ltdr_3` | `float` | `float` |  |  | Accumulated Revaluated Depreciation |
| `tkey_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam800 Transaction |
| `agrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam400 Asset Groups |
| `ctgy_sctg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam220 Subcategory |
| `ctgy_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam200 Categories |
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

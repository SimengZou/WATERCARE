# tffam200

LN tffam200 Categories table, Generated 2026-04-10T19:41:32Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ctgy` |
| **Column count** | 52 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ctgy` | `string` | `varchar` | `PK` | `not_null` | (required) Category. Max length: 10 |
| `agrp` | `string` | `varchar` |  |  | Group. Max length: 10 |
| `amth` | `string` | `varchar` |  |  | AMT Depreciation Method-U.S. Max length: 20 |
| `aslf` | `integer` | `int` |  |  | Asset Life (Units) |
| `bslv` | `integer` | `int` |  |  | Depreciate Below Salvage. Allowed values: 1, 2 |
| `bslv_kw` | `string` | `varchar` |  |  | Depreciate Below Salvage (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cmth` | `string` | `varchar` |  |  | ACE Depreciation Method-U.S. Max length: 20 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `fmth` | `string` | `varchar` |  |  | Financial/Other Depreciation Method. Max length: 20 |
| `itcd` | `integer` | `int` |  |  | ITC Method. Allowed values: 1, 2, 3 |
| `itcd_kw` | `string` | `varchar` |  |  | ITC Method (keyword). Allowed values: tffam.itcc.none, tffam.itcc.redu.basi, tffam.itcc.redu.cred |
| `lmth` | `string` | `varchar` |  |  | Calculatory Deprec. Method. Max length: 20 |
| `life` | `integer` | `int` |  |  | Asset Life |
| `mmth` | `string` | `varchar` |  |  | Commercial Depreciation Method. Max length: 20 |
| `omth` | `string` | `varchar` |  |  | Other Depreciation Method-U.S. Max length: 20 |
| `prop` | `string` | `varchar` |  |  | Property Type. Max length: 10 |
| `sctg` | `string` | `varchar` |  |  | Default Subcategory. Max length: 10 |
| `smth` | `string` | `varchar` |  |  | Special Depreciation Method. Max length: 20 |
| `tmth` | `string` | `varchar` |  |  | Federal Depreciation Method-U.S. Max length: 20 |
| `umth` | `string` | `varchar` |  |  | Statutory Depreciation Method. Max length: 20 |
| `dscr` | `integer` | `int` |  |  | MAPAS Discriminator. Allowed values: 1, 2 |
| `dscr_kw` | `string` | `varchar` |  |  | MAPAS Discriminator (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fadr` | `float` | `float` |  |  | Fiscally Accepted Depreciation Rate |
| `cthr` | `float` | `float` |  |  | Capitalization Threshold |
| `cthc` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `mskc` | `integer` | `int` |  |  | Mask Company |
| `anbm` | `string` | `varchar` |  |  | Asset Number Mask. Max length: 9 |
| `aexm` | `string` | `varchar` |  |  | Asset Extension Mask. Max length: 9 |
| `agrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam400 Asset Groups |
| `amth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `cmth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `fmth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `lmth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `mmth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `omth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `prop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam780 Property Type |
| `smth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `tmth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `umth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam710 Depreciation Method |
| `mskc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `anbm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd402 Masks |
| `aexm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd402 Masks |
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

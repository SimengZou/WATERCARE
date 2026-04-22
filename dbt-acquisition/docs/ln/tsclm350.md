# tsclm350

LN tsclm350 Service Resolution - Probability Analysis table, Generated 2026-04-10T19:42:29Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsclm350` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orig`, `orno`, `acln`, `ccll`, `hidt` |
| **Column count** | 47 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orig` | `integer` | `int` | `PK` | `not_null` | (required) Origin. Allowed values: 1, 2, 3, 4, 5 |
| `orig_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tsclm.orig.cal, tsclm.orig.soc, tsclm.orig.msc, tsclm.orig.wcs, tsclm.orig.cmm |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order Number. Max length: 9 |
| `acln` | `integer` | `int` | `PK` | `not_null` | (required) Activity |
| `ccll` | `string` | `varchar` | `PK` | `not_null` | (required) Call. Max length: 9 |
| `hidt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) History Date |
| `sigr` | `string` | `varchar` |  |  | Serialized Item Group. Max length: 6 |
| `csgr` | `string` | `varchar` |  |  | Service Item Group. Max length: 6 |
| `cgrp` | `string` | `varchar` |  |  | Call Group1. Max length: 3 |
| `scgr` | `string` | `varchar` |  |  | Call Group2. Max length: 3 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Call Description - base language. Max length: 50 |
| `clst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `sern` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `rprl` | `string` | `varchar` |  |  | Reported Problem. Max length: 10 |
| `expr` | `string` | `varchar` |  |  | Expected Problem. Max length: 10 |
| `espr` | `string` | `varchar` |  |  | Actual Problem. Max length: 10 |
| `exsl` | `string` | `varchar` |  |  | Expected Solution. Max length: 10 |
| `sltn` | `string` | `varchar` |  |  | Actual Solution. Max length: 10 |
| `crac` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `sear` | `string` | `varchar` |  |  | Search String. Max length: 132 |
| `ccll_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm100 Calls |
| `sigr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg010 Serialized Item Groups |
| `csgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm210 Service Item Groups |
| `cgrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm050 Call Groups |
| `scgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm050 Call Groups |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `clst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsbsc100 Installation Group |
| `item_sern_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `rprl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm330 Problems |
| `expr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm330 Problems |
| `espr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm330 Problems |
| `exsl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm335 Solutions |
| `sltn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsclm335 Solutions |
| `crac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
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

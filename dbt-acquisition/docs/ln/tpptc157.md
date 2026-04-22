# tpptc157

LN tpptc157 Control Data Subcontracting Lines table, Generated 2026-04-10T19:42:23Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc157` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `csub`, `sern` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `csub` | `string` | `varchar` | `PK` | `not_null` | (required) Subcontracting. Max length: 47 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `ccsb` | `string` | `varchar` |  |  | Control Code Subcontracting. Max length: 47 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `bsqn` | `integer` | `int` |  |  | Budget Line Number |
| `quan` | `float` | `float` |  |  | Quantity |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `cocu` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `dfpr` | `float` | `float` |  |  | Final Price |
| `rtcc_1` | `float` | `float` |  |  | Cost Rate |
| `rtcc_2` | `float` | `float` |  |  | Cost Rate |
| `rtcc_3` | `float` | `float` |  |  | Cost Rate |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Date of Requirement |
| `shft` | `integer` | `int` |  |  | Shifting of Start Date |
| `fidt` | `timestamp` | `timestamp_ntz` |  |  | Planned Finish Date |
| `exep` | `integer` | `int` |  |  | Execute PRP Run. Allowed values: 1, 2 |
| `exep_kw` | `string` | `varchar` |  |  | Execute PRP Run (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `btdt` | `timestamp` | `timestamp_ntz` |  |  | Budget Date |
| `tsrl` | `integer` | `int` |  |  | Service Related. Allowed values: 1, 2 |
| `tsrl_kw` | `string` | `varchar` |  |  | Service Related (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `prby` | `integer` | `int` |  |  | Procurement by. Allowed values: 10, 20, 30 |
| `prby_kw` | `string` | `varchar` |  |  | Procurement by (keyword). Allowed values: tppdm.prby.po, tppdm.prby.sub.po, tppdm.prby.eqp.po |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cocu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
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

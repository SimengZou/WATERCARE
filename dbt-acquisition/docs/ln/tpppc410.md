# tpppc410

LN tpppc410 Cost Control by Project / Material Control Code table, Generated 2026-04-10T19:42:13Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc410` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `ccit` |
| **Column count** | 54 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ccit` | `string` | `varchar` | `PK` | `not_null` | (required) Control Code Material. Max length: 47 |
| `ambg_1` | `float` | `float` |  |  | Total Budget |
| `ambg_2` | `float` | `float` |  |  | Total Budget |
| `ambg_3` | `float` | `float` |  |  | Total Budget |
| `ambg_4` | `float` | `float` |  |  | Total Budget |
| `qubg` | `float` | `float` |  |  | Budget Quantity |
| `ampm_1` | `float` | `float` |  |  | Earned Value |
| `ampm_2` | `float` | `float` |  |  | Earned Value |
| `ampm_3` | `float` | `float` |  |  | Earned Value |
| `ampm_4` | `float` | `float` |  |  | Earned Value |
| `qupm` | `float` | `float` |  |  | Earned Value |
| `amex_1` | `float` | `float` |  |  | Commitments |
| `amex_2` | `float` | `float` |  |  | Commitments |
| `amex_3` | `float` | `float` |  |  | Commitments |
| `amex_4` | `float` | `float` |  |  | Commitments |
| `quex` | `float` | `float` |  |  | Qty Commitments |
| `amac_1` | `float` | `float` |  |  | Actual Cost |
| `amac_2` | `float` | `float` |  |  | Actual Cost |
| `amac_3` | `float` | `float` |  |  | Actual Cost |
| `amac_4` | `float` | `float` |  |  | Actual Cost |
| `quac` | `float` | `float` |  |  | Actual Quantity |
| `ampe_1` | `float` | `float` |  |  | Cost Forecast |
| `ampe_2` | `float` | `float` |  |  | Cost Forecast |
| `ampe_3` | `float` | `float` |  |  | Cost Forecast |
| `ampe_4` | `float` | `float` |  |  | Cost Forecast |
| `qupe` | `float` | `float` |  |  | Forecast Quantity |
| `ampp_1` | `float` | `float` |  |  | Performed (This Period) |
| `ampp_2` | `float` | `float` |  |  | Performed (This Period) |
| `ampp_3` | `float` | `float` |  |  | Performed (This Period) |
| `ampp_4` | `float` | `float` |  |  | Performed (This Period) |
| `qupp` | `float` | `float` |  |  | Performed this Period |
| `amep_1` | `float` | `float` |  |  | Commitments (This Period) |
| `amep_2` | `float` | `float` |  |  | Commitments (This Period) |
| `amep_3` | `float` | `float` |  |  | Commitments (This Period) |
| `amep_4` | `float` | `float` |  |  | Commitments (This Period) |
| `quep` | `float` | `float` |  |  | Qty Commitments this Period |
| `amap_1` | `float` | `float` |  |  | Actual Costs (This Period) |
| `amap_2` | `float` | `float` |  |  | Actual Costs (This Period) |
| `amap_3` | `float` | `float` |  |  | Actual Costs (This Period) |
| `amap_4` | `float` | `float` |  |  | Actual Costs (This Period) |
| `quap` | `float` | `float` |  |  | Actual Qty this Period |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
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

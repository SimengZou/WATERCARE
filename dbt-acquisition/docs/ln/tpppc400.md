# tpppc400

LN tpppc400 Cost Control by Project table, Generated 2026-04-10T19:42:12Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc400` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `mpto` |
| **Column count** | 84 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `mpto` | `integer` | `int` | `PK` | `not_null` | (required) Main Project Total Cost Indicator. Allowed values: 1, 2 |
| `mpto_kw` | `string` | `varchar` |  |  | Main Project Total Cost Indicator (keyword). Allowed values: tppdm.mpto.main.proj.total, tppdm.mpto.proj.total |
| `ambg_1` | `float` | `float` |  |  | Total Budget |
| `ambg_2` | `float` | `float` |  |  | Total Budget |
| `ambg_3` | `float` | `float` |  |  | Total Budget |
| `ambg_4` | `float` | `float` |  |  | Total Budget |
| `ampm_1` | `float` | `float` |  |  | Earned Value |
| `ampm_2` | `float` | `float` |  |  | Earned Value |
| `ampm_3` | `float` | `float` |  |  | Earned Value |
| `ampm_4` | `float` | `float` |  |  | Earned Value |
| `amex_1` | `float` | `float` |  |  | Commitments |
| `amex_2` | `float` | `float` |  |  | Commitments |
| `amex_3` | `float` | `float` |  |  | Commitments |
| `amex_4` | `float` | `float` |  |  | Commitments |
| `amac_1` | `float` | `float` |  |  | Actual Cost |
| `amac_2` | `float` | `float` |  |  | Actual Cost |
| `amac_3` | `float` | `float` |  |  | Actual Cost |
| `amac_4` | `float` | `float` |  |  | Actual Cost |
| `ampe_1` | `float` | `float` |  |  | Cost Forecast |
| `ampe_2` | `float` | `float` |  |  | Cost Forecast |
| `ampe_3` | `float` | `float` |  |  | Cost Forecast |
| `ampe_4` | `float` | `float` |  |  | Cost Forecast |
| `amrs_1` | `float` | `float` |  |  | Revenues |
| `amrs_2` | `float` | `float` |  |  | Revenues |
| `amrs_3` | `float` | `float` |  |  | Revenues |
| `amrs_4` | `float` | `float` |  |  | Revenues |
| `amfc_1` | `float` | `float` |  |  | Costs Reported Completed |
| `amfc_2` | `float` | `float` |  |  | Costs Reported Completed |
| `amfc_3` | `float` | `float` |  |  | Costs Reported Completed |
| `amfc_4` | `float` | `float` |  |  | Costs Reported Completed |
| `amfr_1` | `float` | `float` |  |  | Revenues Reported Completed |
| `amfr_2` | `float` | `float` |  |  | Revenues Reported Completed |
| `amfr_3` | `float` | `float` |  |  | Revenues Reported Completed |
| `amfr_4` | `float` | `float` |  |  | Revenues Reported Completed |
| `ampp_1` | `float` | `float` |  |  | Performed (This Period) |
| `ampp_2` | `float` | `float` |  |  | Performed (This Period) |
| `ampp_3` | `float` | `float` |  |  | Performed (This Period) |
| `ampp_4` | `float` | `float` |  |  | Performed (This Period) |
| `amep_1` | `float` | `float` |  |  | Commitments (This Period) |
| `amep_2` | `float` | `float` |  |  | Commitments (This Period) |
| `amep_3` | `float` | `float` |  |  | Commitments (This Period) |
| `amep_4` | `float` | `float` |  |  | Commitments (This Period) |
| `amap_1` | `float` | `float` |  |  | Actual Costs (This Period) |
| `amap_2` | `float` | `float` |  |  | Actual Costs (This Period) |
| `amap_3` | `float` | `float` |  |  | Actual Costs (This Period) |
| `amap_4` | `float` | `float` |  |  | Actual Costs (This Period) |
| `amrp_1` | `float` | `float` |  |  | Revenues (This Period) |
| `amrp_2` | `float` | `float` |  |  | Revenues (This Period) |
| `amrp_3` | `float` | `float` |  |  | Revenues (This Period) |
| `amrp_4` | `float` | `float` |  |  | Revenues (This Period) |
| `amrf_1` | `float` | `float` |  |  | Forecast Extra Revenue |
| `amrf_2` | `float` | `float` |  |  | Forecast Extra Revenue |
| `amrf_3` | `float` | `float` |  |  | Forecast Extra Revenue |
| `amrf_4` | `float` | `float` |  |  | Forecast Extra Revenue |
| `ambp_1` | `float` | `float` |  |  | Amount Paid to Suppliers |
| `ambp_2` | `float` | `float` |  |  | Amount Paid to Suppliers |
| `ambp_3` | `float` | `float` |  |  | Amount Paid to Suppliers |
| `ambp_4` | `float` | `float` |  |  | Amount Paid to Suppliers |
| `ambr_1` | `float` | `float` |  |  | Amount Paid by Customers |
| `ambr_2` | `float` | `float` |  |  | Amount Paid by Customers |
| `ambr_3` | `float` | `float` |  |  | Amount Paid by Customers |
| `ambr_4` | `float` | `float` |  |  | Amount Paid by Customers |
| `amds_1` | `float` | `float` |  |  | Amount Due to Suppliers |
| `amds_2` | `float` | `float` |  |  | Amount Due to Suppliers |
| `amds_3` | `float` | `float` |  |  | Amount Due to Suppliers |
| `amds_4` | `float` | `float` |  |  | Amount Due to Suppliers |
| `amdr_1` | `float` | `float` |  |  | Amount Due by Customers |
| `amdr_2` | `float` | `float` |  |  | Amount Due by Customers |
| `amdr_3` | `float` | `float` |  |  | Amount Due by Customers |
| `amdr_4` | `float` | `float` |  |  | Amount Due by Customers |
| `mprj` | `string` | `varchar` |  |  | Main Project. Max length: 9 |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `mprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
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

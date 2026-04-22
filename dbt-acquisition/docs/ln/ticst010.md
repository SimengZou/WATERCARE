# ticst010

LN ticst010 End Item Unit Costs table, Generated 2026-04-10T19:41:47Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_ticst010` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pdno`, `cstv`, `cwoc`, `cpcp`, `addc`, `csto` |
| **Column count** | 42 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pdno` | `string` | `varchar` | `PK` | `not_null` | (required) Production Order. Max length: 9 |
| `cstv` | `integer` | `int` | `PK` | `not_null` | (required) Cost View. Allowed values: 1, 2 |
| `cstv_kw` | `string` | `varchar` |  |  | Cost View (keyword). Allowed values: ticst.cstv.item.based, ticst.cstv.cwoc.based |
| `cwoc` | `string` | `varchar` | `PK` | `not_null` | (required) Work Center. Max length: 6 |
| `cpcp` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Component. Max length: 8 |
| `addc` | `integer` | `int` | `PK` | `not_null` | (required) Added Costs. Allowed values: 1, 2 |
| `addc_kw` | `string` | `varchar` |  |  | Added Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `csto` | `integer` | `int` | `PK` | `not_null` | (required) Customer Owned. Allowed values: 1, 2 |
| `csto_kw` | `string` | `varchar` |  |  | Customer Owned (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `nune` | `float` | `float` |  |  | Estimated Number of Units |
| `eamt_1` | `float` | `float` |  |  | Estimated Amount |
| `eamt_2` | `float` | `float` |  |  | Estimated Amount |
| `eamt_3` | `float` | `float` |  |  | Estimated Amount |
| `nuna` | `float` | `float` |  |  | Actual Number of Units |
| `aamt_1` | `float` | `float` |  |  | Actual Amount |
| `aamt_2` | `float` | `float` |  |  | Actual Amount |
| `aamt_3` | `float` | `float` |  |  | Actual Amount |
| `pdno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tisfc001 Production Orders |
| `cpcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `eamt_lclc` | `float` | `float` |  |  | CC: Estimated Amount Local Currency |
| `eamt_rpc1` | `float` | `float` |  |  | CC: Estimated Amount Reporting Currency 1 |
| `eamt_rpc2` | `float` | `float` |  |  | CC: Estimated Amount Reporting Currency 2 |
| `eamt_refc` | `float` | `float` |  |  | CC: Estimated Amount Reference Currency |
| `eamt_dwhc` | `float` | `float` |  |  | CC: Estimated Amount in Data Warehouse Currency |
| `aamt_lclc` | `float` | `float` |  |  | CC: Actual Amount in Local Currency |
| `aamt_rpc1` | `float` | `float` |  |  | CC: Actual Amount in Reporting Currency 1 |
| `aamt_rpc2` | `float` | `float` |  |  | CC: Actual Amount in Reporting Currency 2 |
| `aamt_refc` | `float` | `float` |  |  | CC: Actual Amount in Reference Currency |
| `aamt_dwhc` | `float` | `float` |  |  | CC: Actual Amount in Data Warehouse Currency |
| `dptm_fcmp` | `integer` | `int` |  |  | CC: Financial Company of Department |
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

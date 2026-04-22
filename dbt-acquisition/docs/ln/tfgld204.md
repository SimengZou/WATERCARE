# tfgld204

LN tfgld204 Opening Balances - Dimension/Ledger/Currency table, Generated 2026-04-10T19:41:44Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld204` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `year`, `dtyp`, `dimx`, `leac`, `ccur`, `duac`, `bpid` |
| **Column count** | 32 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Fiscal Year |
| `dtyp` | `integer` | `int` | `PK` | `not_null` | (required) Dimension Type |
| `dimx` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension. Max length: 9 |
| `leac` | `string` | `varchar` | `PK` | `not_null` | (required) Ledger Account. Max length: 12 |
| `ccur` | `string` | `varchar` | `PK` | `not_null` | (required) Currency. Max length: 3 |
| `duac` | `integer` | `int` | `PK` | `not_null` | (required) Dual Accounting Indicator. Allowed values: 0, 1, 2 |
| `duac_kw` | `string` | `varchar` |  |  | Dual Accounting Indicator (keyword). Allowed values: , tfgld.duac.statutory, tfgld.duac.complem |
| `bpid` | `string` | `varchar` | `PK` | `not_null` | (required) Business Partner. Max length: 9 |
| `ftob` | `float` | `float` |  |  | Opening Balance Finalized Transactions |
| `fobh_1` | `float` | `float` |  |  | Opening Balance Finalized Transactions in Home Currency |
| `fobh_2` | `float` | `float` |  |  | Opening Balance Finalized Transactions in Home Currency |
| `fobh_3` | `float` | `float` |  |  | Opening Balance Finalized Transactions in Home Currency |
| `ntob` | `float` | `float` |  |  | Opening Balance Not Final Transactions |
| `nobh_1` | `float` | `float` |  |  | Opening Balance Not Finalized Transactions in HC |
| `nobh_2` | `float` | `float` |  |  | Opening Balance Not Finalized Transactions in HC |
| `nobh_3` | `float` | `float` |  |  | Opening Balance Not Finalized Transactions in HC |
| `qob1` | `float` | `float` |  |  | Opening Balance Finalized Quantity 1 |
| `qob2` | `float` | `float` |  |  | Opening Balance Finalized Quantity 2 |
| `nob1` | `float` | `float` |  |  | Opening Balance Not Final Quantity 1 |
| `nob2` | `float` | `float` |  |  | Opening Balance Not Final Quantity 2 |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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

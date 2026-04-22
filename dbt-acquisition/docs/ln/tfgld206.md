# tfgld206

LN tfgld206 Opening Balances - Combination of Dimension/Ledger/Currency table, Generated 2026-04-10T19:41:45Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld206` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `year`, `dim1`, `dim2`, `dim3`, `dim4`, `dim5`, `dims`, `leac`, `ccur`, `duac`, `bpid` |
| **Column count** | 50 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Fiscal Year |
| `dim1` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 1. Max length: 9 |
| `dim2` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 2. Max length: 9 |
| `dim3` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 3. Max length: 9 |
| `dim4` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 4. Max length: 9 |
| `dim5` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `dims` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 6-12. Max length: 63 |
| `leac` | `string` | `varchar` | `PK` | `not_null` | (required) Ledger Account. Max length: 12 |
| `ccur` | `string` | `varchar` | `PK` | `not_null` | (required) Currency. Max length: 3 |
| `duac` | `integer` | `int` | `PK` | `not_null` | (required) Dual Accounting Indicator. Allowed values: 0, 1, 2 |
| `duac_kw` | `string` | `varchar` |  |  | Dual Accounting Indicator (keyword). Allowed values: , tfgld.duac.statutory, tfgld.duac.complem |
| `bpid` | `string` | `varchar` | `PK` | `not_null` | (required) Business Partner. Max length: 9 |
| `ftob` | `float` | `float` |  |  | Opening Balance Finalized Transactions |
| `fobh_1` | `float` | `float` |  |  | Opening Balance Finalized Transactions in HC |
| `fobh_2` | `float` | `float` |  |  | Opening Balance Finalized Transactions in HC |
| `fobh_3` | `float` | `float` |  |  | Opening Balance Finalized Transactions in HC |
| `ntob` | `float` | `float` |  |  | Opening Balance Not Final Transactions |
| `nobh_1` | `float` | `float` |  |  | Opening Balance Not Finalized Transactions in HC |
| `nobh_2` | `float` | `float` |  |  | Opening Balance Not Finalized Transactions in HC |
| `nobh_3` | `float` | `float` |  |  | Opening Balance Not Finalized Transactions in HC |
| `qob1` | `float` | `float` |  |  | Opening Balance Finalized Quantity 1 |
| `qob2` | `float` | `float` |  |  | Opening Balance Finalized Quantity 2 |
| `nob1` | `float` | `float` |  |  | Opening Balance Not Final Quantity 1 |
| `nob2` | `float` | `float` |  |  | Opening Balance Not Final Quantity 2 |
| `olap` | `integer` | `int` |  |  | Send to OLAP |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `fobh_rfrc` | `float` | `float` |  |  | CC: Opening Balance Finalized Transactions in Reference Currency |
| `fobh_dtwc` | `float` | `float` |  |  | CC: Opening Balance Finalized Transactions in Data Warehouse Currency |
| `nobh_rfrc` | `float` | `float` |  |  | CC: Opening Balance Not Finalized Transactions in Reference Currency |
| `nobh_dtwc` | `float` | `float` |  |  | CC: Opening Balance Not Finalized Transactions in Data Warehouse Curre |
| `dimx_sgm1` | `string` | `varchar` |  |  | CC: Opening Balance Dimension Segment 1. Max length: 9 |
| `dimx_sgm2` | `string` | `varchar` |  |  | CC: Opening Balance Dimension Segment 2. Max length: 9 |
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

# tffst121

LN tffst121 Statement Ledger/Dimension Accounts table, Generated 2026-04-10T19:41:39Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffst121` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `fstm`, `accn`, `seqn` |
| **Column count** | 57 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `fstm` | `string` | `varchar` | `PK` | `not_null` | (required) Financial Statement. Max length: 8 |
| `accn` | `string` | `varchar` | `PK` | `not_null` | (required) Statement Account. Max length: 12 |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cmpm` | `string` | `varchar` |  |  | Company mask. Max length: 20 |
| `cmpf` | `integer` | `int` |  |  | From Company |
| `cmpt` | `integer` | `int` |  |  | To Company |
| `lacm` | `string` | `varchar` |  |  | Ledger Account / Cash Flow Reason mask. Max length: 20 |
| `lacf` | `string` | `varchar` |  |  | From Ledger Account / Cash Flow Reason. Max length: 12 |
| `lact` | `string` | `varchar` |  |  | To Ledger Account / Cash Flow Reason. Max length: 12 |
| `dimm_1` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_2` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_3` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_4` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_5` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_6` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_7` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_8` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_9` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_10` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_11` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimm_12` | `string` | `varchar` |  |  | Dimension mask. Max length: 20 |
| `dimf_1` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_2` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_3` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_4` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_5` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_6` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_7` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_8` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_9` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_10` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_11` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimf_12` | `string` | `varchar` |  |  | From Dimension. Max length: 9 |
| `dimt_1` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_2` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_3` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_4` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_5` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_6` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_7` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_8` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_9` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_10` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_11` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `dimt_12` | `string` | `varchar` |  |  | To Dimension. Max length: 9 |
| `fstm_accn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst120 Statement Accounts |
| `fstm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffst100 Financial Statements |
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

# tffam120

LN tffam120 Asset Distribution table, Generated 2026-04-10T19:41:32Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam120` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `anbr`, `aext`, `dkey` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `anbr` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Extension. Max length: 15 |
| `dkey` | `integer` | `int` | `PK` | `not_null` | (required) Asset Distribution Key |
| `comp` | `integer` | `int` |  |  | Company |
| `dqty` | `integer` | `int` |  |  | Quantity |
| `lkey` | `integer` | `int` |  |  | Location Key |
| `memo__en_us` | `string` | `varchar` |  | `not_null` | (required) Memo - base language. Max length: 30 |
| `srce` | `integer` | `int` |  |  | Source. Allowed values: 1, 2 |
| `srce_kw` | `string` | `varchar` |  |  | Source (keyword). Allowed values: tffam.srce.inte, tffam.srce.tran |
| `trsc` | `string` | `varchar` |  |  | Transaction Template. Max length: 3 |
| `anbr_aext_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam100 Asset |
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

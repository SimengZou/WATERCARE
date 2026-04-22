# tffam805

LN tffam805 Transaction Distribution table, Generated 2026-04-10T19:41:34Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam805` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `anbr`, `aext`, `book`, `tkey`, `bpid`, `lkey` |
| **Column count** | 29 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `anbr` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Extension. Max length: 15 |
| `book` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Book. Max length: 10 |
| `tkey` | `integer` | `int` | `PK` | `not_null` | (required) Transaction Key |
| `bpid` | `string` | `varchar` | `PK` | `not_null` | (required) Business Partner. Max length: 9 |
| `lkey` | `integer` | `int` | `PK` | `not_null` | (required) Location Key |
| `cost_1` | `float` | `float` |  |  | Cost |
| `cost_2` | `float` | `float` |  |  | Cost |
| `cost_3` | `float` | `float` |  |  | Cost |
| `depr_1` | `float` | `float` |  |  | Depreciation |
| `depr_2` | `float` | `float` |  |  | Depreciation |
| `depr_3` | `float` | `float` |  |  | Depreciation |
| `rcst_1` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_2` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_3` | `float` | `float` |  |  | Revaluation Cost |
| `rdpr_1` | `float` | `float` |  |  | Revaluation Depreciation |
| `rdpr_2` | `float` | `float` |  |  | Revaluation Depreciation |
| `rdpr_3` | `float` | `float` |  |  | Revaluation Depreciation |
| `tkey_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam800 Transaction |
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

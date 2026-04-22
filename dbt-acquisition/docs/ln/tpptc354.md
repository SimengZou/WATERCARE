# tpptc354

LN tpptc354 Actual Budget by Sundry Cost Control Code table, Generated 2026-04-10T19:42:27Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc354` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `ccal`, `ccic` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ccal` | `string` | `varchar` | `PK` | `not_null` | (required) Budget Cost Analysis Version. Max length: 3 |
| `ccic` | `string` | `varchar` | `PK` | `not_null` | (required) Control Code Sundry Cost. Max length: 8 |
| `ames_1` | `float` | `float` |  |  | Sundry Costs Amount |
| `ames_2` | `float` | `float` |  |  | Sundry Costs Amount |
| `ames_3` | `float` | `float` |  |  | Sundry Costs Amount |
| `ames_4` | `float` | `float` |  |  | Sundry Costs Amount |
| `amch_1` | `float` | `float` |  |  | Surcharge Amount |
| `amch_2` | `float` | `float` |  |  | Surcharge Amount |
| `amch_3` | `float` | `float` |  |  | Surcharge Amount |
| `amch_4` | `float` | `float` |  |  | Surcharge Amount |
| `quan` | `float` | `float` |  |  | Quantity |
| `cprj_ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc300 Budget Cost Analysis Versions by Project |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
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

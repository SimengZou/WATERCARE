# tcmcs028

LN tcmcs028 Harmonized System Codes table, Generated 2026-04-10T19:41:11Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs028` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ccde` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ccde` | `string` | `varchar` | `PK` | `not_null` | (required) Harmonized System Code. Max length: 25 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 250 |
| `unit` | `string` | `varchar` |  |  | Reporting Unit. Max length: 3 |
| `quan` | `float` | `float` |  |  | Quantity per Reporting Unit |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `txta` | `integer` | `int` |  |  | Text |
| `cdf_ccde__en_us` | `string` | `varchar` |  | `not_null` | (required) Category Code - base language. Max length: 6 |
| `cdf_cdes__en_us` | `string` | `varchar` |  | `not_null` | (required) Category Description - base language. Max length: 60 |
| `cdf_gccd__en_us` | `string` | `varchar` |  | `not_null` | (required) Group Category Code - base language. Max length: 6 |
| `cdf_gdsc__en_us` | `string` | `varchar` |  | `not_null` | (required) Group Category Description - base language. Max length: 60 |
| `unit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

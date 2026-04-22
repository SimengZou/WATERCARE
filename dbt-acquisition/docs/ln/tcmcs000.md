# tcmcs000

LN tcmcs000 MCS Parameters table, Generated 2026-04-10T19:41:08Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs000` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt` |
| **Column count** | 25 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Introduction Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `cwei` | `string` | `varchar` |  |  | Base Weight Unit. Max length: 3 |
| `clen` | `string` | `varchar` |  |  | Base Length Unit. Max length: 3 |
| `care` | `string` | `varchar` |  |  | Base Area Unit. Max length: 3 |
| `cvol` | `string` | `varchar` |  |  | Base Volume Unit. Max length: 3 |
| `ctim` | `string` | `varchar` |  |  | Base Time Unit. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Default Exchange Rate Type. Max length: 3 |
| `logn` | `string` | `varchar` |  |  | Logname. Max length: 16 |
| `cwei_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `clen_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `care_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cvol_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ctim_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
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

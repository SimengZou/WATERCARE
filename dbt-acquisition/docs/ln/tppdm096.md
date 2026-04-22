# tppdm096

LN tppdm096 User Defined Structure Elements table, Generated 2026-04-10T19:41:58Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm096` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cobs`, `coel` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cobs` | `string` | `varchar` | `PK` | `not_null` | (required) User Defined Structure. Max length: 9 |
| `coel` | `string` | `varchar` | `PK` | `not_null` | (required) User Defined Structure Element. Max length: 8 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `coep` | `string` | `varchar` |  |  | Parent Element. Max length: 8 |
| `emno` | `string` | `varchar` |  |  | Responsible Person. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Text |
| `cobs_coep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm096 User Defined Structure Elements |
| `cobs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm095 User Defined Structures |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
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

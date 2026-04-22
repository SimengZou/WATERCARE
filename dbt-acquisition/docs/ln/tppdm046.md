# tppdm046

LN tppdm046 Third Parties table, Generated 2026-04-10T19:41:56Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm046` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cint`, `cins` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cint` | `string` | `varchar` | `PK` | `not_null` | (required) Third Party Type. Max length: 3 |
| `cins` | `string` | `varchar` | `PK` | `not_null` | (required) Third Party. Max length: 3 |
| `name__en_us` | `string` | `varchar` |  | `not_null` | (required) Name - base language. Max length: 30 |
| `ccnt` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `padr` | `string` | `varchar` |  |  | Postal Address Code. Max length: 9 |
| `vadr` | `string` | `varchar` |  |  | Visiting Address Code. Max length: 9 |
| `cint_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm045 Third Party Types |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `padr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `vadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
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

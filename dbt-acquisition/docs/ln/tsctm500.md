# tsctm500

LN tsctm500 Generic Warranties table, Generated 2026-04-10T19:42:35Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm500` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `gwte` |
| **Column count** | 28 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `gwte` | `string` | `varchar` | `PK` | `not_null` | (required) Generic Warranty. Max length: 16 |
| `cwte` | `string` | `varchar` |  |  | Warranty Template. Max length: 16 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `uprd` | `integer` | `int` |  |  | Use Period for Serialized Item. Allowed values: 1, 2 |
| `uprd_kw` | `string` | `varchar` |  |  | Use Period for Serialized Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nrpe` | `integer` | `int` |  |  | Period |
| `peru` | `integer` | `int` |  |  | Period Unit. Allowed values: 5, 10, 15, 20, 25 |
| `peru_kw` | `string` | `varchar` |  |  | Period Unit (keyword). Allowed values: tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `cwoc` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `term` | `integer` | `int` |  |  | Term ID |
| `efdt` | `date` | `date` |  |  | Effective Date |
| `exdt` | `date` | `date` |  |  | Expiry Date |
| `txta` | `integer` | `int` |  |  | Text |
| `cwte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm020 Warranty Templates |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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

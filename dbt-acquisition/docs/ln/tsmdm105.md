# tsmdm105

LN tsmdm105 Service Areas table, Generated 2026-04-10T19:42:36Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsmdm105` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `csar` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `csar` | `string` | `varchar` | `PK` | `not_null` | (required) Service Area. Max length: 3 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `artp` | `integer` | `int` |  |  | Service Area Type. Allowed values: 5, 10 |
| `artp_kw` | `string` | `varchar` |  |  | Service Area Type (keyword). Allowed values: tsmdm.artp.main, tsmdm.artp.sub |
| `cmsa` | `string` | `varchar` |  |  | Main Service Area. Max length: 3 |
| `cwoc` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `trti` | `float` | `float` |  |  | Average Traveling Time |
| `dist` | `float` | `float` |  |  | Average Traveling Distance |
| `ccha` | `float` | `float` |  |  | Call-out Charge |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `clrt` | `string` | `varchar` |  |  | Labor Rate. Max length: 6 |
| `cmsa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm105 Service Areas |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `clrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
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

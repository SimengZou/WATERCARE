# tcemm030

LN tcemm030 Enterprise Units table, Generated 2026-04-10T19:41:05Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcemm030` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `eunt` |
| **Column count** | 29 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `eunt` | `string` | `varchar` | `PK` | `not_null` | (required) Enterprise Unit. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `euca` | `string` | `varchar` |  |  | Enterprise Unit Category. Max length: 6 |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `dfpo` | `string` | `varchar` |  |  | Default Purchase Office. Max length: 6 |
| `dfco` | `string` | `varchar` |  |  | Default Calculation Office. Max length: 6 |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `duns` | `integer` | `int` |  |  | DUNS Number |
| `txtn` | `integer` | `int` |  |  | Text |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `fcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `euca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm032 Enterprise Unit Categories |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `txtn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

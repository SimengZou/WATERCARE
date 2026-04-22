# tppdm016

LN tppdm016 Trade Groups table, Generated 2026-04-10T19:41:54Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm016` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ctrg` |
| **Column count** | 25 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ctrg` | `string` | `varchar` | `PK` | `not_null` | (required) Trade Group. Max length: 6 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `clrt` | `string` | `varchar` |  |  | Labor Rate Code. Max length: 6 |
| `ltlr` | `timestamp` | `timestamp_ntz` |  |  | Labor Rate Code Transaction Date |
| `nuem` | `float` | `float` |  |  | Number of Employees |
| `mine` | `float` | `float` |  |  | Minimum Number of Employees |
| `mane` | `float` | `float` |  |  | Maximum Number of Employees |
| `dcpu` | `float` | `float` |  |  | Normal Cap. Utilization by Day |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `mtrg` | `string` | `varchar` |  |  | Main Trade Group. Max length: 6 |
| `avtp` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `clrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `mtrg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm016 Trade Groups |
| `avtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
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

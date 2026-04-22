# tccom001

LN tccom001 Employees - General table, Generated 2026-04-10T19:41:00Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `emno` |
| **Column count** | 32 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `emno` | `string` | `varchar` | `PK` | `not_null` | (required) Employee. Max length: 9 |
| `nama__en_us` | `string` | `varchar` |  | `not_null` | (required) Name - base language. Max length: 35 |
| `namb__en_us` | `string` | `varchar` |  | `not_null` | (required) Given Name - base language. Max length: 50 |
| `namc__en_us` | `string` | `varchar` |  | `not_null` | (required) Middle Name - base language. Max length: 50 |
| `namd__en_us` | `string` | `varchar` |  | `not_null` | (required) Last Name - base language. Max length: 50 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `cwoc` | `string` | `varchar` |  |  | Department. Max length: 6 |
| `clrt` | `string` | `varchar` |  |  | Labor Rate. Max length: 6 |
| `cpcp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `loco` | `string` | `varchar` |  |  | Logon Code. Max length: 16 |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `guid` | `string` | `varchar` |  |  | GUID. Max length: 22 |
| `ctit` | `string` | `varchar` |  |  | Title. Max length: 3 |
| `cdf_emno` | `string` | `varchar` |  |  | Old Employee Code. Max length: 9 |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `clrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl090 Labor Rate Codes |
| `cpcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ctit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs019 Titles |
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

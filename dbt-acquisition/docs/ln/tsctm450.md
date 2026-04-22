# tsctm450

LN tsctm450 Estimated Contract Revenues per Period table, Generated 2026-04-10T19:42:34Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm450` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `csco`, `cchn`, `cfln`, `fsyr`, `fspr` |
| **Column count** | 37 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `csco` | `string` | `varchar` | `PK` | `not_null` | (required) Service Contract. Max length: 9 |
| `cchn` | `integer` | `int` | `PK` | `not_null` | (required) Origin Contract Change |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Configuration Line |
| `fsyr` | `integer` | `int` | `PK` | `not_null` | (required) Year |
| `fspr` | `integer` | `int` | `PK` | `not_null` | (required) Period |
| `nody` | `integer` | `int` |  |  | Number of Days |
| `cvdy` | `integer` | `int` |  |  | Covered days |
| `esrv` | `float` | `float` |  |  | Estimated Revenue |
| `rcrv` | `float` | `float` |  |  | Recognized Revenue |
| `esco_1` | `float` | `float` |  |  | Obsolete |
| `esco_2` | `float` | `float` |  |  | Obsolete |
| `esco_3` | `float` | `float` |  |  | Obsolete |
| `acpp_1` | `float` | `float` |  |  | Obsolete |
| `acpp_2` | `float` | `float` |  |  | Obsolete |
| `acpp_3` | `float` | `float` |  |  | Obsolete |
| `csco_cchn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `esrv_homc` | `float` | `float` |  |  | CC: Estimated Revenue Amount in Local Currency |
| `esrv_rpc1` | `float` | `float` |  |  | CC: Estimated Revenue Amount in Reporting Currency 1 |
| `esrv_rpc2` | `float` | `float` |  |  | CC: Estimated Revenue Amount in Reporting Currency 2 |
| `esrv_refc` | `float` | `float` |  |  | CC: Estimated Revenue Amount in Reference Currency |
| `esrv_dwhc` | `float` | `float` |  |  | CC: Estimated Revenue Amount in Data Warehouse Currency |
| `rcrv_homc` | `float` | `float` |  |  | CC: Recognized Revenue Amount in Local Currency |
| `rcrv_rpc1` | `float` | `float` |  |  | CC: Recognized Revenue Amount in Reporting Currency 1 |
| `rcrv_rpc2` | `float` | `float` |  |  | CC: Recognized Revenue Amount in Reporting Currency 2 |
| `rcrv_refc` | `float` | `float` |  |  | CC: Recognized Revenue Amount in Reference Currency |
| `rcrv_dwhc` | `float` | `float` |  |  | CC: Recognized Revenue Amount in Data Warehouse Currency |
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

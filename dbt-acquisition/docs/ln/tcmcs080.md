# tcmcs080

LN tcmcs080 Carriers/LSP table, Generated 2026-04-10T19:41:14Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs080` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cfrw` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cfrw` | `string` | `varchar` | `PK` | `not_null` | (required) Carrier/LSP. Max length: 3 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `suno` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `trmd` | `integer` | `int` |  |  | Transport Category. Allowed values: 10, 15, 20, 25, 30, 35, 40, 45, 50, 52, 54, 56, 58, 60, 62, 70, 80, 83, 85, 87, 90, 93, 96, 98, 100 |
| `trmd_kw` | `string` | `varchar` |  |  | Transport Category (keyword). Allowed values: tccom.trmd.water, tccom.trmd.water.cont, tccom.trmd.rail, tccom.trmd.rail.cont, tccom.trmd.road, tccom.trmd.road.cont, tccom.trmd.air, tccom.trmd.air.charter, tccom.trmd.post, tccom.trmd.contract.carr, tccom.trmd.customer.pickup, tccom.trmd.truckload, tccom.trmd.mail, tccom.trmd.intermodal, tccom.trmd.consolidation, tccom.trmd.instal, tccom.trmd.inlwater, tccom.trmd.express.air, tccom.trmd.express.truck, tccom.trmd.express.rail, tccom.trmd.own, tccom.trmd.pool.point, tccom.trmd.milk.run, tccom.trmd.piggy.back, tccom.trmd.none |
| `scac` | `string` | `varchar` |  |  | Standard Carrier Alpha Code. Max length: 20 |
| `suno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
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

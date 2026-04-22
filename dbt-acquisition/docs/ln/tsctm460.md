# tsctm460

LN tsctm460 Contract Revenues table, Generated 2026-04-10T19:42:34Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm460` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `csco`, `cchn`, `cfln`, `plyr`, `plpr`, `seqn` |
| **Column count** | 54 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `csco` | `string` | `varchar` | `PK` | `not_null` | (required) Service Contract. Max length: 9 |
| `cchn` | `integer` | `int` | `PK` | `not_null` | (required) Origin Contract Change |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Configuration Line |
| `plyr` | `integer` | `int` | `PK` | `not_null` | (required) Planned Year |
| `plpr` | `integer` | `int` | `PK` | `not_null` | (required) Planned Period |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Revenue Line |
| `ndrc` | `integer` | `int` |  |  | Number of Days Recognized |
| `clrv` | `float` | `float` |  |  | Calculated Revenue |
| `corv` | `float` | `float` |  |  | Contract Revenue |
| `rahc_1` | `float` | `float` |  |  | Revenue Amount in Home Currency |
| `rahc_2` | `float` | `float` |  |  | Revenue Amount in Home Currency |
| `rahc_3` | `float` | `float` |  |  | Revenue Amount in Home Currency |
| `ratc_1` | `float` | `float` |  |  | Currency Rates for Home Currencies |
| `ratc_2` | `float` | `float` |  |  | Currency Rates for Home Currencies |
| `ratc_3` | `float` | `float` |  |  | Currency Rates for Home Currencies |
| `ratf_1` | `integer` | `int` |  |  | Rate Factors for Home Currencies |
| `ratf_2` | `integer` | `int` |  |  | Rate Factors for Home Currencies |
| `ratf_3` | `integer` | `int` |  |  | Rate Factors for Home Currencies |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtor` | `integer` | `int` |  |  | Rate Origin. Allowed values: 1, 5, 10, 99 |
| `rtor_kw` | `string` | `varchar` |  |  | Rate Origin (keyword). Allowed values: tsctm.rtor.currency.rates, tsctm.rtor.manual, tsctm.rtor.calculated, tsctm.rtor.not.applicable |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `cmdt` | `timestamp` | `timestamp_ntz` |  |  | Confirm Date |
| `cmur` | `string` | `varchar` |  |  | Confirmed by User. Max length: 16 |
| `rcdt` | `timestamp` | `timestamp_ntz` |  |  | Recognition Date |
| `rcur` | `string` | `varchar` |  |  | Recognized by User. Max length: 16 |
| `poyr` | `integer` | `int` |  |  | Posted Fiscal Year |
| `popr` | `integer` | `int` |  |  | Posted Fiscal Period |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 5, 10, 15, 20 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tsctm.rvst.free, tsctm.rvst.confirm, tsctm.rvst.recognize, tsctm.rvst.posted |
| `erfa` | `float` | `float` |  |  | Earned Revenue Factor |
| `prov` | `float` | `float` |  |  | Provision |
| `rchn` | `integer` | `int` |  |  | Change Number of Contract Period |
| `camt_1` | `float` | `float` |  |  | Obsolete |
| `camt_2` | `float` | `float` |  |  | Obsolete |
| `camt_3` | `float` | `float` |  |  | Obsolete |
| `acco_1` | `float` | `float` |  |  | Obsolete |
| `acco_2` | `float` | `float` |  |  | Obsolete |
| `acco_3` | `float` | `float` |  |  | Obsolete |
| `csco_cchn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `csco_rchn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `corv_refc` | `float` | `float` |  |  | CC: Revenue Amount in Reference Currency |
| `corv_dwhc` | `float` | `float` |  |  | CC: Revenue Amount in Data Warehouse Currency |
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

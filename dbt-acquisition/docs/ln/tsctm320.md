# tsctm320

LN tsctm320 Contract Changes table, Generated 2026-04-10T19:42:33Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm320` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `csco`, `cchn` |
| **Column count** | 51 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `csco` | `string` | `varchar` | `PK` | `not_null` | (required) Service Contract. Max length: 9 |
| `cchn` | `integer` | `int` | `PK` | `not_null` | (required) Contract Change Number |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `actn` | `integer` | `int` |  |  | Type. Allowed values: 0, 5, 10, 12, 15, 20 |
| `actn_kw` | `string` | `varchar` |  |  | Type (keyword). Allowed values: , tsctm.chng.original, tsctm.chng.renewed, tsctm.chng.renew.with.indx, tsctm.chng.incidental, tsctm.chng.indexation |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 0, 5, 7 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: , tsctm.stat.open, tsctm.stat.active |
| `term` | `integer` | `int` |  |  | Contract Terms |
| `nrpe` | `integer` | `int` |  |  | Change Period |
| `peru` | `integer` | `int` |  |  | Change Period Unit. Allowed values: 0, 5, 10, 15, 20, 25 |
| `peru_kw` | `string` | `varchar` |  |  | Change Period Unit (keyword). Allowed values: , tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `ochn` | `integer` | `int` |  |  | Origin Contract Change Number |
| `icpn` | `float` | `float` |  |  | Penalty |
| `efdt` | `date` | `date` |  |  | Effective Date |
| `exdt` | `date` | `date` |  |  | Expiry Date |
| `chdt` | `date` | `date` |  |  | Change Effective Date |
| `cind` | `string` | `varchar` |  |  | Indexation Template. Max length: 3 |
| `crtm` | `timestamp` | `timestamp_ntz` |  |  | Creation Time |
| `csmt` | `float` | `float` |  |  | Calculated Sales Amount |
| `rsmt` | `float` | `float` |  |  | Sales Amount |
| `camt_1` | `float` | `float` |  |  | Cost Amount |
| `camt_2` | `float` | `float` |  |  | Cost Amount |
| `camt_3` | `float` | `float` |  |  | Cost Amount |
| `erfa` | `float` | `float` |  |  | Earned Revenue Factor |
| `amdy` | `float` | `float` |  |  | Amount per Day |
| `cody_1` | `float` | `float` |  |  | Cost per Day |
| `cody_2` | `float` | `float` |  |  | Cost per Day |
| `cody_3` | `float` | `float` |  |  | Cost per Day |
| `inpc` | `float` | `float` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Contract Change Text |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `cind_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm060 Indexation Templates |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `rsmt_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `rsmt_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `rsmt_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `rsmt_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `rsmt_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
| `camt_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `camt_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `camt_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
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

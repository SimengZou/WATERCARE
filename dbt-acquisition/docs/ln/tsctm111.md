# tsctm111

LN tsctm111 Fixed Price Terms table, Generated 2026-04-10T19:42:30Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm111` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `term`, `cfln`, `cseq` |
| **Column count** | 36 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `term` | `integer` | `int` | `PK` | `not_null` | (required) TermID |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Configuration Line |
| `cseq` | `integer` | `int` | `PK` | `not_null` | (required) Fixed Price Line |
| `fplv` | `integer` | `int` |  |  | Price Level. Allowed values: 5, 10, 15, 20, 25 |
| `fplv_kw` | `string` | `varchar` |  |  | Price Level (keyword). Allowed values: tsctm.fplv.ref.activity, tsctm.fplv.master.routing, tsctm.fplv.routing.option, tsctm.fplv.any.activity, tsctm.fplv.any.order |
| `cact` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `camr` | `string` | `varchar` |  |  | Master Routing. Max length: 16 |
| `caro` | `string` | `varchar` |  |  | Routing Option. Max length: 16 |
| `pris` | `float` | `float` |  |  | Sales Price |
| `txta` | `integer` | `int` |  |  | Text |
| `term_cfln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm110 Configuration Lines |
| `cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `camr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `caro_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `pris_homc` | `float` | `float` |  |  | CC: Sales Price in Local Currency |
| `pris_rpc1` | `float` | `float` |  |  | CC: Sales Price in Reporting Currency 1 |
| `pris_rpc2` | `float` | `float` |  |  | CC: Sales Price in Reporting Currency 2 |
| `pris_refc` | `float` | `float` |  |  | CC: Sales Price in Reference Currency |
| `pris_dwhc` | `float` | `float` |  |  | CC: Sales Price in Data Warehouse Currency |
| `camt_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `camt_homc` | `float` | `float` |  |  | CC: Cost Amount in Local Currency |
| `camt_rpc1` | `float` | `float` |  |  | CC: Cost Amount in Reporting Currency 1 |
| `camt_rpc2` | `float` | `float` |  |  | CC: Cost Amount in Reporting Currency 2 |
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

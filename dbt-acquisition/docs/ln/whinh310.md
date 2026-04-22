# whinh310

LN whinh310 Receipt Headers table, Generated 2026-04-10T19:42:48Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh310` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `rcno` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `rcno` | `string` | `varchar` | `PK` | `not_null` | (required) Receipt. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `carr` | `string` | `varchar` |  |  | Delivery Carrier/LSP. Max length: 3 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `shda` | `string` | `varchar` |  |  | Delivery Address. Max length: 9 |
| `pddt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `shdt` | `timestamp` | `timestamp_ntz` |  |  | Shipping Date |
| `fval` | `float` | `float` |  |  | Freight Value |
| `curr` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `wght` | `float` | `float` |  |  | Gross Weight |
| `cwun` | `string` | `varchar` |  |  | Unit of Measure. Max length: 3 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `dino__en_us` | `string` | `varchar` |  | `not_null` | (required) Packing Slip - base language. Max length: 30 |
| `conf` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `conf_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `load` | `string` | `varchar` |  |  | Load. Max length: 9 |
| `shid` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `imre__en_us` | `string` | `varchar` |  | `not_null` | (required) Import Reference - base language. Max length: 30 |
| `irdt` | `timestamp` | `timestamp_ntz` |  |  | Import Reference Date |
| `imrk__en_us` | `string` | `varchar` |  | `not_null` | (required) Import Reference Key - base language. Max length: 30 |
| `port` | `string` | `varchar` |  |  | Port of Entry. Max length: 9 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30, 100 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: whinh.rhst.open, whinh.rhst.confirmed, whinh.rhst.under.review, whinh.rhst.not.appl |
| `tcap` | `integer` | `int` |  |  | Localized Authorization Procedure Applicable. Allowed values: 1, 2 |
| `tcap_kw` | `string` | `varchar` |  |  | Localized Authorization Procedure Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tccp` | `integer` | `int` |  |  | Localized Authorization Procedure Complete. Allowed values: 1, 2 |
| `tccp_kw` | `string` | `varchar` |  |  | Localized Authorization Procedure Complete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trcd` | `string` | `varchar` |  |  | Authorization Number. Max length: 50 |
| `text` | `integer` | `int` |  |  | Text |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `carr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `shda_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `curr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `huid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd530 Handling Units |
| `port_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs043 Ports |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

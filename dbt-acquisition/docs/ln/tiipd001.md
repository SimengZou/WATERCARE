# tiipd001

LN tiipd001 Items - Production table, Generated 2026-04-10T19:41:47Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tiipd001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item` |
| **Column count** | 69 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cpha` | `integer` | `int` |  |  | Phantom. Allowed values: 1, 2 |
| `cpha_kw` | `string` | `varchar` |  |  | Phantom (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oqdr` | `integer` | `int` |  |  | Quantity-dependent Routing. Allowed values: 1, 2 |
| `oqdr_kw` | `string` | `varchar` |  |  | Quantity-dependent Routing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `phst` | `integer` | `int` |  |  | Use Phantom Inventory. Allowed values: 1, 2 |
| `phst_kw` | `string` | `varchar` |  |  | Use Phantom Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iuma` | `integer` | `int` |  |  | Turn Operation 0 into First of. Allowed values: 10, 20 |
| `iuma_kw` | `string` | `varchar` |  |  | Turn Operation 0 into First of (keyword). Allowed values: tiipd.iuma.oper1.oper.item, tiipd.iuma.oper1.prod.ordr |
| `repi` | `integer` | `int` |  |  | Repetitive Item. Allowed values: 1, 2 |
| `repi_kw` | `string` | `varchar` |  |  | Repetitive Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scdl` | `string` | `varchar` |  |  | Schedule Code. Max length: 3 |
| `pcrp` | `integer` | `int` |  |  | Rate Factor for Planning |
| `bfcp` | `integer` | `int` |  |  | Backflush if Material. Allowed values: 1, 2 |
| `bfcp_kw` | `string` | `varchar` |  |  | Backflush if Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bfep` | `integer` | `int` |  |  | Backflush Materials. Allowed values: 1, 2 |
| `bfep_kw` | `string` | `varchar` |  |  | Backflush Materials (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bfhr` | `integer` | `int` |  |  | Backflush Hours. Allowed values: 1, 2 |
| `bfhr_kw` | `string` | `varchar` |  |  | Backflush Hours (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `unom` | `float` | `float` |  |  | BOM Quantity |
| `runi` | `float` | `float` |  |  | Routing Quantity |
| `scpf` | `float` | `float` |  |  | Scrap Percentage |
| `scpq` | `float` | `float` |  |  | Scrap Quantity |
| `drin` | `integer` | `int` |  |  | Direct Initiate Inventory Issue. Allowed values: 1, 2 |
| `drin_kw` | `string` | `varchar` |  |  | Direct Initiate Inventory Issue (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dris` | `integer` | `int` |  |  | Direct Process Warehouse Order Line. Allowed values: 1, 2 |
| `dris_kw` | `string` | `varchar` |  |  | Direct Process Warehouse Order Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `stoi` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `stoi_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `coac` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `coac_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bomu` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `bomu_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nsfc` | `integer` | `int` |  |  | Net Change JSC. Allowed values: 1, 2 |
| `nsfc_kw` | `string` | `varchar` |  |  | Net Change JSC (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oltm` | `float` | `float` |  |  | Order Lead Time |
| `oltu` | `integer` | `int` |  |  | Order Lead Time Unit. Allowed values: 5, 10 |
| `oltu_kw` | `string` | `varchar` |  |  | Order Lead Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `swoc` | `integer` | `int` |  |  | Warehouse is Mandatory on BOM Line. Allowed values: 1, 2 |
| `swoc_kw` | `string` | `varchar` |  |  | Warehouse is Mandatory on BOM Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cick` | `integer` | `int` |  |  | Critical for Inventory. Allowed values: 1, 2 |
| `cick_kw` | `string` | `varchar` |  |  | Critical for Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `smfs` | `integer` | `int` |  |  | Subcontracting with Material Flow. Allowed values: 1, 2 |
| `smfs_kw` | `string` | `varchar` |  |  | Subcontracting with Material Flow (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iimf` | `integer` | `int` |  |  | Receipt Inspection. Allowed values: 1, 2 |
| `iimf_kw` | `string` | `varchar` |  |  | Receipt Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iima` | `integer` | `int` |  |  | Outbound Inspection. Allowed values: 1, 2 |
| `iima_kw` | `string` | `varchar` |  |  | Outbound Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `sfpl` | `string` | `varchar` |  |  | Shop Floor Planner. Max length: 9 |
| `rgrp` | `string` | `varchar` |  |  | Routing Group. Max length: 6 |
| `jsst` | `string` | `varchar` |  |  | Job Shop Site. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Item Production Text |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `scdl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tirpt001 Production Schedule Codes |
| `sfpl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `rgrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs145 Routing Groups |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

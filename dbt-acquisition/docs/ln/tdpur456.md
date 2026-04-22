# tdpur456

LN tdpur456 Purchase Actual Receipt History table, Generated 2026-04-10T19:41:23Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur456` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `pono`, `sqnb`, `rsqn`, `trdt`, `sern` |
| **Column count** | 82 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `rsqn` | `integer` | `int` | `PK` | `not_null` | (required) Receipt Sequence Number |
| `trdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Date |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Serial Number |
| `hisa` | `integer` | `int` |  |  | History Action. Allowed values: 10, 20, 30, 40, 90 |
| `hisa_kw` | `string` | `varchar` |  |  | History Action (keyword). Allowed values: tcactn.created, tcactn.changed, tcactn.deleted, tcactn.canceled, tcactn.not.appl |
| `logn` | `string` | `varchar` |  |  | Login Code. Max length: 16 |
| `ddte` | `timestamp` | `timestamp_ntz` |  |  | Actual Receipt Date |
| `rcld` | `timestamp` | `timestamp_ntz` |  |  | Receipt Log Date |
| `shdt` | `timestamp` | `timestamp_ntz` |  |  | Shipping Date |
| `rcno` | `string` | `varchar` |  |  | Receipt Number. Max length: 9 |
| `rseq` | `integer` | `int` |  |  | Receipt Number Sequence |
| `dino__en_us` | `string` | `varchar` |  | `not_null` | (required) Packing Slip - base language. Max length: 30 |
| `qpdl` | `float` | `float` |  |  | Received Quantity in Piece Unit |
| `qidl` | `float` | `float` |  |  | Received Quantity |
| `qpps` | `float` | `float` |  |  | Packing Slip Quantity in Piece Unit |
| `qips` | `float` | `float` |  |  | Packing Slip Quantity |
| `qpap` | `float` | `float` |  |  | Approved Quantity in Piece Unit |
| `qiap` | `float` | `float` |  |  | Approved Quantity |
| `qprj` | `float` | `float` |  |  | Rejected Quantity in Piece Unit |
| `qirj` | `float` | `float` |  |  | Rejected Quantity |
| `crej` | `string` | `varchar` |  |  | Reason for Rejection. Max length: 6 |
| `cuva` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `lssn` | `string` | `varchar` |  |  | Item Identification Set. Max length: 22 |
| `copr_1` | `float` | `float` |  |  | Standard Cost |
| `copr_2` | `float` | `float` |  |  | Standard Cost |
| `copr_3` | `float` | `float` |  |  | Standard Cost |
| `coop_1` | `float` | `float` |  |  | Operation Costs |
| `coop_2` | `float` | `float` |  |  | Operation Costs |
| `coop_3` | `float` | `float` |  |  | Operation Costs |
| `damt` | `float` | `float` |  |  | Receipt Amount |
| `amld` | `float` | `float` |  |  | Order Line Discount Amount |
| `amod` | `float` | `float` |  |  | Order Discount Amount |
| `amch` | `float` | `float` |  |  | Amount Change |
| `rarc` | `integer` | `int` |  |  | Reason for Amount Change. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8 |
| `rarc_kw` | `string` | `varchar` |  |  | Reason for Amount Change (keyword). Allowed values: tdpur.rach.receipt, tdpur.rach.rec.corr, tdpur.rach.inspection, tdpur.rach.issue, tdpur.rach.pric.change, tdpur.rach.not.appl, tdpur.rach.consumption, tdpur.rach.proc.pur.order |
| `fire` | `integer` | `int` |  |  | Final Receipt. Allowed values: 1, 2 |
| `fire_kw` | `string` | `varchar` |  |  | Final Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `arej` | `integer` | `int` |  |  | Accepted Rejected Goods. Allowed values: 1, 2 |
| `arej_kw` | `string` | `varchar` |  |  | Accepted Rejected Goods (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qual` | `integer` | `int` |  |  | Inspection. Allowed values: 1, 2 |
| `qual_kw` | `string` | `varchar` |  |  | Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ftdt` | `timestamp` | `timestamp_ntz` |  |  | Finance Transaction Date |
| `afrw` | `string` | `varchar` |  |  | Actual Carrier. Max length: 3 |
| `load` | `string` | `varchar` |  |  | Load. Max length: 9 |
| `shpm` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `ldat` | `timestamp` | `timestamp_ntz` |  |  | Load Date |
| `wght` | `float` | `float` |  |  | Weight |
| `wtun` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `refa__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 30 |
| `deln` | `string` | `varchar` |  |  | Delivery Note. Max length: 19 |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `revi` | `string` | `varchar` |  |  | Engineering Item Revision. Max length: 6 |
| `citr__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item Revision - base language. Max length: 35 |
| `asno__en_us` | `string` | `varchar` |  | `not_null` | (required) ASN - base language. Max length: 30 |
| `asnl__en_us` | `string` | `varchar` |  | `not_null` | (required) ASN Line - base language. Max length: 30 |
| `onpr` | `float` | `float` |  |  | Original Net Price in Order Currency |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Text |
| `crej_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `lssn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd410 Item Identification Sets |
| `wtun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `mpnr_cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu045 Manufacturer Part Numbers |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
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

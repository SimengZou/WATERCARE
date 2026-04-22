# tdpur406

LN tdpur406 Purchase Actual Receipts table, Generated 2026-04-10T19:41:22Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur406` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `pono`, `sqnb`, `rsqn` |
| **Column count** | 82 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `rsqn` | `integer` | `int` | `PK` | `not_null` | (required) Receipt Sequence Number |
| `ddte` | `timestamp` | `timestamp_ntz` |  |  | Actual Receipt Date |
| `rcld` | `timestamp` | `timestamp_ntz` |  |  | Receipt Log Date |
| `shdt` | `timestamp` | `timestamp_ntz` |  |  | Shipping Date |
| `rcno` | `string` | `varchar` |  |  | Receipt Number. Max length: 9 |
| `rseq` | `integer` | `int` |  |  | Receipt Line |
| `dino__en_us` | `string` | `varchar` |  | `not_null` | (required) Packing Slip - base language. Max length: 30 |
| `qpdl` | `float` | `float` |  |  | Received Quantity in Piece Unit |
| `qidl` | `float` | `float` |  |  | Received Quantity |
| `qpps` | `float` | `float` |  |  | Packing Slip Quantity in Piece Unit |
| `qips` | `float` | `float` |  |  | Packing Slip Quantity |
| `asno__en_us` | `string` | `varchar` |  | `not_null` | (required) Supplier ASN Number - base language. Max length: 30 |
| `asnl__en_us` | `string` | `varchar` |  | `not_null` | (required) Suppliers ASN Line - base language. Max length: 30 |
| `qpap` | `float` | `float` |  |  | Approved Quantity in Piece Unit |
| `qiap` | `float` | `float` |  |  | Approved Quantity |
| `qprj` | `float` | `float` |  |  | Rejected Quantity in Piece Unit |
| `qirj` | `float` | `float` |  |  | Rejected Quantity |
| `crej` | `string` | `varchar` |  |  | Reason for Rejection. Max length: 6 |
| `cuva` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `lssn` | `string` | `varchar` |  |  | Item Identification Set. Max length: 22 |
| `damt` | `float` | `float` |  |  | Receipt Amount |
| `amld` | `float` | `float` |  |  | Order Line Discount Amount |
| `amod` | `float` | `float` |  |  | Order Discount Amount |
| `copr_1` | `float` | `float` |  |  | Standard Cost |
| `copr_2` | `float` | `float` |  |  | Standard Cost |
| `copr_3` | `float` | `float` |  |  | Standard Cost |
| `coop_1` | `float` | `float` |  |  | Operation Costs |
| `coop_2` | `float` | `float` |  |  | Operation Costs |
| `coop_3` | `float` | `float` |  |  | Operation Costs |
| `fire` | `integer` | `int` |  |  | Final Receipt. Allowed values: 1, 2 |
| `fire_kw` | `string` | `varchar` |  |  | Final Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `conf` | `integer` | `int` |  |  | Confirmed. Allowed values: 1, 2 |
| `conf_kw` | `string` | `varchar` |  |  | Confirmed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `arej` | `integer` | `int` |  |  | Accepted Rejected Goods. Allowed values: 1, 2 |
| `arej_kw` | `string` | `varchar` |  |  | Accepted Rejected Goods (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qual` | `integer` | `int` |  |  | Inspection. Allowed values: 1, 2 |
| `qual_kw` | `string` | `varchar` |  |  | Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
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
| `onpr` | `float` | `float` |  |  | Original Net Price in Order Currency |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `cdf_adat` | `timestamp` | `timestamp_ntz` |  |  | Approved Date |
| `cdf_aerr` | `string` | `varchar` |  |  | Approval Error. Max length: 100 |
| `cdf_amnt` | `float` | `float` |  |  | Correction Quantity |
| `cdf_ausr` | `string` | `varchar` |  |  | Approved By. Max length: 16 |
| `cdf_idet__en_us` | `string` | `varchar` |  | `not_null` | (required) Item Detail - base language. Max length: 100 |
| `cdf_wfst` | `integer` | `int` |  |  | Workflow Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `cdf_wfst_kw` | `string` | `varchar` |  |  | Workflow Status (keyword). Allowed values: tdcdf_lst003.not.applicable, tdcdf_lst003.approved, tdcdf_lst003.rejected, tdcdf_lst003.pending, tdcdf_lst003.recall, tdcdf_lst003.appr.received |
| `orno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur400 Purchase Orders |
| `crej_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `lssn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd410 Item Identification Sets |
| `afrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
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

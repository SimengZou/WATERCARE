# tfacp245

LN tfacp245 Goods Receipts table, Generated 2026-04-10T19:41:29Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp245` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `shpm`, `otyp`, `orno`, `pono`, `sqnb`, `loco`, `rcno`, `rseq` |
| **Column count** | 60 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `shpm` | `string` | `varchar` | `PK` | `not_null` | (required) Packing Slip. Max length: 30 |
| `otyp` | `integer` | `int` | `PK` | `not_null` | (required) Order Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 |
| `otyp_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tfacp.otyp.purchase, tfacp.otyp.sales, tfacp.otyp.trans, tfacp.otyp.trans.man, tfacp.otyp.trans.wip, tfacp.otyp.freight, tfacp.otyp.commissions, tfacp.otyp.project, tfacp.otyp.project.man, tfacp.otyp.ent.planning, tfacp.otyp.assembly, tfacp.otyp.production, tfacp.otyp.service, tfacp.otyp.maint.sales, tfacp.otyp.pcs.project, tfacp.otyp.maint.work, tfacp.otyp.not.applicable, tfacp.otyp.proj.contract, tfacp.otyp.customer.claim, tfacp.otyp.intercomp.trade, tfacp.otyp.services.procm |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Order Position |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `loco` | `integer` | `int` | `PK` | `not_null` | (required) Logistic Company |
| `rcno` | `string` | `varchar` | `PK` | `not_null` | (required) Goods Receipt. Max length: 9 |
| `rseq` | `integer` | `int` | `PK` | `not_null` | (required) Receipt Line |
| `bonm` | `string` | `varchar` |  |  | Obsolete. Max length: 17 |
| `boid` | `string` | `varchar` |  |  | Obsolete. Max length: 11 |
| `borf` | `string` | `varchar` |  |  | Obsolete. Max length: 90 |
| `rgui` | `string` | `varchar` |  |  | Obsolete. Max length: 22 |
| `fcpo` | `integer` | `int` |  |  | Financial Company Purchase Office |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `rqty` | `float` | `float` |  |  | Received Quantity in Inventory Unit |
| `rqpc` | `float` | `float` |  |  | Received Quantity in Piece Unit |
| `quan` | `float` | `float` |  |  | Approved Quantity in Inventory Unit (Goods) |
| `aqpc` | `float` | `float` |  |  | Approved Quantity in Piece Unit |
| `trqn` | `float` | `float` |  |  | Rejected Quantity in Inventory Unit |
| `rjpc` | `float` | `float` |  |  | Rejected Quantity in Piece Unit |
| `toma` | `integer` | `int` |  |  | Totally Matched. Allowed values: 1, 2 |
| `toma_kw` | `string` | `varchar` |  |  | Totally Matched (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sfbl` | `integer` | `int` |  |  | Self-billed/Internal Order. Allowed values: 1, 2 |
| `sfbl_kw` | `string` | `varchar` |  |  | Self-billed/Internal Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Goods Rcpt Date |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Shipping Date |
| `sbdt` | `integer` | `int` |  |  | Self Billing Date Type. Allowed values: 0, 10, 20 |
| `sbdt_kw` | `string` | `varchar` |  |  | Self Billing Date Type (keyword). Allowed values: , tcsbdt.receipt.date, tcsbdt.shipping.date |
| `imrf__en_us` | `string` | `varchar` |  | `not_null` | (required) Import Reference - base language. Max length: 30 |
| `mtch` | `integer` | `int` |  |  | Match. Allowed values: 1, 2 |
| `mtch_kw` | `string` | `varchar` |  |  | Match (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `maqu` | `float` | `float` |  |  | Match Quantity |
| `mqpc` | `float` | `float` |  |  | Match Quantity in Piece Unit |
| `mqpu` | `float` | `float` |  |  | Match Quantity in Purchase Unit |
| `maam` | `float` | `float` |  |  | Match Amount |
| `lmat` | `integer` | `int` |  |  | Level of Match. Allowed values: 10, 20 |
| `lmat_kw` | `string` | `varchar` |  |  | Level of Match (keyword). Allowed values: tfacp.lmat.inv.header, tfacp.lmat.inv.line |
| `omti` | `integer` | `int` |  |  | Overwrite Match of Invoice Header. Allowed values: 1, 2 |
| `omti_kw` | `string` | `varchar` |  |  | Overwrite Match of Invoice Header (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uppr` | `integer` | `int` |  |  | Update Average/Last Purchase Price. Allowed values: 1, 2 |
| `uppr_kw` | `string` | `varchar` |  |  | Update Average/Last Purchase Price (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cval` | `float` | `float` |  |  | Customs Value |
| `cvlc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `cdf_txta` | `integer` | `int` |  |  | Text |
| `loco_otyp_orno_pono_sqnb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfacp240 Order Data for Approval |
| `cvlc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cdf_txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

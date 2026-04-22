# tfacp251

LN tfacp251 Invoices Related To Purchase Goods Receipt Lines table, Generated 2026-04-10T19:41:29Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp251` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `icom`, `ityp`, `idoc`, `loco`, `shpm`, `otyp`, `orno`, `pono`, `sqnb`, `rcno`, `rseq` |
| **Column count** | 55 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `icom` | `integer` | `int` | `PK` | `not_null` | (required) Financial Company Purchase Invoice |
| `ityp` | `string` | `varchar` | `PK` | `not_null` | (required) Invoice Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` | `PK` | `not_null` | (required) Invoice Document |
| `loco` | `integer` | `int` | `PK` | `not_null` | (required) Logistic Company |
| `shpm` | `string` | `varchar` | `PK` | `not_null` | (required) Packing Slip. Max length: 30 |
| `otyp` | `integer` | `int` | `PK` | `not_null` | (required) Order Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 |
| `otyp_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tfacp.otyp.purchase, tfacp.otyp.sales, tfacp.otyp.trans, tfacp.otyp.trans.man, tfacp.otyp.trans.wip, tfacp.otyp.freight, tfacp.otyp.commissions, tfacp.otyp.project, tfacp.otyp.project.man, tfacp.otyp.ent.planning, tfacp.otyp.assembly, tfacp.otyp.production, tfacp.otyp.service, tfacp.otyp.maint.sales, tfacp.otyp.pcs.project, tfacp.otyp.maint.work, tfacp.otyp.not.applicable, tfacp.otyp.proj.contract, tfacp.otyp.customer.claim, tfacp.otyp.intercomp.trade, tfacp.otyp.services.procm |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Position |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `rcno` | `string` | `varchar` | `PK` | `not_null` | (required) Goods Receipt. Max length: 9 |
| `rseq` | `integer` | `int` | `PK` | `not_null` | (required) Receipt Line |
| `ccur` | `string` | `varchar` |  |  | Invoice Currency. Max length: 3 |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `iamt` | `float` | `float` |  |  | Invoiced Amount |
| `iqan` | `float` | `float` |  |  | Invoiced Quantity |
| `iqpc` | `float` | `float` |  |  | Invoiced Quantity in Piece Unit |
| `amth_1` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_2` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_3` | `float` | `float` |  |  | Amount in Home Currency |
| `finl` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `finl_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `pric` | `integer` | `int` |  |  | Update Average/Last Purchase Price. Allowed values: 0, 1, 2 |
| `pric_kw` | `string` | `varchar` |  |  | Update Average/Last Purchase Price (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `data` | `timestamp` | `timestamp_ntz` |  |  | Date of Approval |
| `apry` | `integer` | `int` |  |  | Approval Year |
| `aprp` | `integer` | `int` |  |  | Approval Period |
| `dbmo` | `integer` | `int` |  |  | Debit Memo. Allowed values: 1, 2 |
| `dbmo_kw` | `string` | `varchar` |  |  | Debit Memo (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `buex` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `buex_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imrf__en_us` | `string` | `varchar` |  | `not_null` | (required) Import Reference - base language. Max length: 30 |
| `pdif` | `float` | `float` |  |  | Price Difference |
| `pdfd` | `integer` | `int` |  |  | Price Difference Determined. Allowed values: 1, 2 |
| `pdfd_kw` | `string` | `varchar` |  |  | Price Difference Determined (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shpm_otyp_orno_pono_sqnb_loco_rcno_rseq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfacp245 Goods Receipts |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
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

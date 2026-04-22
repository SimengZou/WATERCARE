# tfacp256

LN tfacp256 Matching Data by Invoice table, Generated 2026-04-10T19:41:30Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp256` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `icom`, `ityp`, `idoc`, `ilin`, `idln`, `iseq` |
| **Column count** | 99 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `icom` | `integer` | `int` | `PK` | `not_null` | (required) Financial Company Purchase Invoice |
| `ityp` | `string` | `varchar` | `PK` | `not_null` | (required) Invoice Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` | `PK` | `not_null` | (required) Invoice Document |
| `ilin` | `integer` | `int` | `PK` | `not_null` | (required) Segment/Payment Line Number |
| `idln` | `integer` | `int` | `PK` | `not_null` | (required) Invoice Detail Line |
| `iseq` | `integer` | `int` | `PK` | `not_null` | (required) Invoice Sequence |
| `sort` | `integer` | `int` |  |  | Sort Order |
| `lmat` | `integer` | `int` |  |  | Level of Match. Allowed values: 10, 20 |
| `lmat_kw` | `string` | `varchar` |  |  | Level of Match (keyword). Allowed values: tfacp.lmat.inv.header, tfacp.lmat.inv.line |
| `mtyp` | `integer` | `int` |  |  | Matching Type. Allowed values: 5, 10, 20, 30, 40, 50, 60, 65, 70 |
| `mtyp_kw` | `string` | `varchar` |  |  | Matching Type (keyword). Allowed values: tfacp.mtyp.add.costs, tfacp.mtyp.order, tfacp.mtyp.receipt, tfacp.mtyp.consumption, tfacp.mtyp.landed.cost, tfacp.mtyp.freight, tfacp.mtyp.stage.payment, tfacp.mtyp.services.procm, tfacp.mtyp.not.applicable |
| `mtrc` | `integer` | `int` |  |  | Matching Result Complete. Allowed values: 1, 2 |
| `mtrc_kw` | `string` | `varchar` |  |  | Matching Result Complete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `loco` | `integer` | `int` |  |  | Logistic Company |
| `otyp` | `integer` | `int` |  |  | Order Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21 |
| `otyp_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: tfacp.otyp.purchase, tfacp.otyp.sales, tfacp.otyp.trans, tfacp.otyp.trans.man, tfacp.otyp.trans.wip, tfacp.otyp.freight, tfacp.otyp.commissions, tfacp.otyp.project, tfacp.otyp.project.man, tfacp.otyp.ent.planning, tfacp.otyp.assembly, tfacp.otyp.production, tfacp.otyp.service, tfacp.otyp.maint.sales, tfacp.otyp.pcs.project, tfacp.otyp.maint.work, tfacp.otyp.not.applicable, tfacp.otyp.proj.contract, tfacp.otyp.customer.claim, tfacp.otyp.intercomp.trade, tfacp.otyp.services.procm |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `afpn` | `string` | `varchar` |  |  | Application for Payment. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Position |
| `sqnb` | `integer` | `int` |  |  | Sequence Number |
| `vrln` | `integer` | `int` |  |  | Variation Line |
| `clin` | `integer` | `int` |  |  | Call-Off Line |
| `rlin` | `integer` | `int` |  |  | Retention Line Number |
| `rttp` | `integer` | `int` |  |  | Retention Type. Allowed values: 10, 20, 255 |
| `rttp_kw` | `string` | `varchar` |  |  | Retention Type (keyword). Allowed values: tcrttp.deducted, tcrttp.released, tcrttp.not.appl |
| `rcno` | `string` | `varchar` |  |  | Goods Receipt. Max length: 9 |
| `rseq` | `integer` | `int` |  |  | Receipt Line |
| `shpm__en_us` | `string` | `varchar` |  | `not_null` | (required) Packing Slip - base language. Max length: 30 |
| `cseq` | `integer` | `int` |  |  | Consumption Sequence |
| `cisq` | `integer` | `int` |  |  | Consumption Inspection Sequence |
| `lcln` | `integer` | `int` |  |  | Landed Cost Line Number |
| `spln` | `integer` | `int` |  |  | Stage Payment Line |
| `boty` | `integer` | `int` |  |  | Business Object Type. Allowed values: 1, 2, 3, 6, 7, 10, 11, 15, 16, 20, 21, 30, 31, 32, 33, 34, 40, 41, 90 |
| `boty_kw` | `string` | `varchar` |  |  | Business Object Type (keyword). Allowed values: tclct.bo.type.tdpur400, tclct.bo.type.tdpur401, tclct.bo.type.tdpur406, tclct.bo.type.tdpur311, tclct.bo.type.tdpur315, tclct.bo.type.whinh200, tclct.bo.type.whinh210, tclct.bo.type.whinh300, tclct.bo.type.whinh301, tclct.bo.type.whinh310, tclct.bo.type.whinh312, tclct.bo.type.tdpur105, tclct.bo.type.tdpur106, tclct.bo.type.tdpur109.r, tclct.bo.type.tdpur109.c, tclct.bo.type.tdpur371, tclct.bo.type.tdpcg200, tclct.bo.type.tdpcg201, tclct.bo.type.not.appl |
| `boor` | `integer` | `int` |  |  | Business Object Origin. Allowed values: 1, 2, 10, 20, 25, 30, 31, 100 |
| `boor_kw` | `string` | `varchar` |  |  | Business Object Origin (keyword). Allowed values: tclct.boor.wh.trf, tclct.boor.wh.trf.man, tclct.boor.ep.distr, tclct.boor.sales, tclct.boor.sales.sched, tclct.boor.project, tclct.boor.project.man, tclct.boor.not.appl |
| `bobj` | `string` | `varchar` |  |  | Business Object. Max length: 25 |
| `borf` | `string` | `varchar` |  |  | Business Object Reference. Max length: 90 |
| `load` | `string` | `varchar` |  |  | Load. Max length: 9 |
| `shpi` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `clus` | `string` | `varchar` |  |  | Freight Order Cluster. Max length: 9 |
| `shln` | `integer` | `int` |  |  | Shipment Line |
| `clln` | `integer` | `int` |  |  | Freight Order Cluster Line |
| `iamt` | `float` | `float` |  |  | Invoiced Amount |
| `icur` | `string` | `varchar` |  |  | Invoice Currency. Max length: 3 |
| `iqan` | `float` | `float` |  |  | Invoiced Quantity |
| `iqpc` | `float` | `float` |  |  | Invoiced Quantity in Piece Unit |
| `maam` | `float` | `float` |  |  | Matched Landed Cost Amount in IC |
| `rdam` | `float` | `float` |  |  | Retention Discount Amount |
| `data` | `timestamp` | `timestamp_ntz` |  |  | Date of Approval |
| `apry` | `integer` | `int` |  |  | Approval Year |
| `aprp` | `integer` | `int` |  |  | Approval Period |
| `pric` | `integer` | `int` |  |  | Update Average/Last Purchase Price. Allowed values: 0, 1, 2 |
| `pric_kw` | `string` | `varchar` |  |  | Update Average/Last Purchase Price (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `oamt` | `float` | `float` |  |  | Order Amount in IC |
| `ocur` | `string` | `varchar` |  |  | Order Currency. Max length: 3 |
| `oqan` | `float` | `float` |  |  | Order Quantity in inventory unit |
| `oqpc` | `float` | `float` |  |  | Order Quantity in Piece Unit |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `iuni` | `string` | `varchar` |  |  | Inventory Unit. Max length: 3 |
| `itdo` | `string` | `varchar` |  |  | Invoice Related Transaction Type. Max length: 3 |
| `pcun` | `string` | `varchar` |  |  | Piece Unit. Max length: 3 |
| `idcn` | `integer` | `int` |  |  | Invoice Related Document Number |
| `ilno` | `integer` | `int` |  |  | Invoice Related Line Number |
| `vatc` | `string` | `varchar` |  |  | Matched Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Matched Tax Code. Max length: 9 |
| `tbai` | `float` | `float` |  |  | Matched Tax Base Amount in IC |
| `ovtc` | `string` | `varchar` |  |  | Tax Country from Order. Max length: 3 |
| `ocvt` | `string` | `varchar` |  |  | Tax Code from Order. Max length: 9 |
| `cvli` | `float` | `float` |  |  | Customs Value in Invoice Currency |
| `sern` | `integer` | `int` |  |  | Additional Cost Transaction Number |
| `dbcr` | `integer` | `int` |  |  | Debit/Credit. Allowed values: 1, 2 |
| `dbcr_kw` | `string` | `varchar` |  |  | Debit/Credit (keyword). Allowed values: tfgld.dbcr.debit, tfgld.dbcr.credit |
| `trgu` | `string` | `varchar` |  |  | GUID. Max length: 22 |
| `tctc` | `integer` | `int` |  |  | Tax Consistent. Allowed values: 10, 20, 30, 40, 255 |
| `tctc_kw` | `string` | `varchar` |  |  | Tax Consistent (keyword). Allowed values: tfacp.tcnl.not.checked, tfacp.tcnl.no.match, tfacp.tcnl.accepted, tfacp.tcnl.yes, tfacp.tcnl.not.appl |
| `tcrd` | `integer` | `int` |  |  | Registration Data Consistent. Allowed values: 10, 20, 30, 40, 255 |
| `tcrd_kw` | `string` | `varchar` |  |  | Registration Data Consistent (keyword). Allowed values: tfacp.tcnl.not.checked, tfacp.tcnl.no.match, tfacp.tcnl.accepted, tfacp.tcnl.yes, tfacp.tcnl.not.appl |
| `tcna` | `integer` | `int` |  |  | Net Amount Consistent. Allowed values: 10, 20, 30, 40, 255 |
| `tcna_kw` | `string` | `varchar` |  |  | Net Amount Consistent (keyword). Allowed values: tfacp.tcnl.not.checked, tfacp.tcnl.no.match, tfacp.tcnl.accepted, tfacp.tcnl.yes, tfacp.tcnl.not.appl |
| `tctb` | `integer` | `int` |  |  | Tax Base Amount Consistent. Allowed values: 10, 20, 30, 40, 255 |
| `tctb_kw` | `string` | `varchar` |  |  | Tax Base Amount Consistent (keyword). Allowed values: tfacp.tcnl.not.checked, tfacp.tcnl.no.match, tfacp.tcnl.accepted, tfacp.tcnl.yes, tfacp.tcnl.not.appl |
| `vatc_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `vatc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `ovtc_ocvt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `ovtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ocvt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
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

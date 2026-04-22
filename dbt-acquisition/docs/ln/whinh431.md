# whinh431

LN whinh431 Shipment Lines table, Generated 2026-04-10T19:42:50Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh431` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `shpm`, `pono` |
| **Column count** | 160 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `shpm` | `string` | `varchar` | `PK` | `not_null` | (required) Shipment. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Shipment Line |
| `shst` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 10, 20, 30, 40 |
| `shst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: whinh.shst.open, whinh.shst.frozen, whinh.shst.confirmed, whinh.shst.part.frozen, whinh.shst.confirming, whinh.shst.projected, whinh.shst.canceled |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `sttg` | `string` | `varchar` |  |  | Staged Tag. Max length: 9 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `idat` | `timestamp` | `timestamp_ntz` |  |  | Inventory Date |
| `cuni` | `string` | `varchar` |  |  | Inventory Unit. Max length: 3 |
| `qadv` | `float` | `float` |  |  | Advised Quantity |
| `qadp` | `float` | `float` |  |  | Advised Quantity in Piece Unit |
| `qpic` | `float` | `float` |  |  | Staged Quantity |
| `qppu` | `float` | `float` |  |  | Staged Quantity in Piece Unit |
| `qshp` | `float` | `float` |  |  | Shipped Quantity in Inventory Unit |
| `qspu` | `float` | `float` |  |  | Shipped Quantity in Piece Unit |
| `qnsh` | `float` | `float` |  |  | Not Shipped Quantity |
| `qnpu` | `float` | `float` |  |  | Not Shipped Quantity in Piece Unit |
| `qprv` | `float` | `float` |  |  | Projected Quantity in Inventory Unit |
| `qprp` | `float` | `float` |  |  | Projected Quantity in Piece Unit |
| `adjs` | `integer` | `int` |  |  | Automatic Adjustment of Quantity Not Shipped. Allowed values: 1, 2 |
| `adjs_kw` | `string` | `varchar` |  |  | Automatic Adjustment of Quantity Not Shipped (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `recd` | `string` | `varchar` |  |  | Reason for Adjustment. Max length: 6 |
| `qrcp` | `float` | `float` |  |  | EDI Confirmed Received Quantity |
| `grwt` | `float` | `float` |  |  | Gross Weight |
| `ntwt` | `float` | `float` |  |  | Net Weight |
| `cwun` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `pbol` | `integer` | `int` |  |  | Print Bills of Lading. Allowed values: 1, 2 |
| `pbol_kw` | `string` | `varchar` |  |  | Print Bills of Lading (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bolp` | `integer` | `int` |  |  | Cluster Line |
| `worg` | `integer` | `int` |  |  | Order Origin. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `worg_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `worn` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `wset` | `integer` | `int` |  |  | Set |
| `wpon` | `integer` | `int` |  |  | Order Line |
| `wseq` | `integer` | `int` |  |  | Sequence |
| `boml` | `integer` | `int` |  |  | BOM Line |
| `rseq` | `integer` | `int` |  |  | Sales Sequence |
| `pwsq` | `integer` | `int` |  |  | Planned Delivery Line Sequence |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` |  |  | Staging Location. Max length: 10 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 30 |
| `refs__en_us` | `string` | `varchar` |  | `not_null` | (required) Shipment Reference - base language. Max length: 35 |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 40 |
| `iadj` | `integer` | `int` |  |  | Inventory Updated. Allowed values: 1, 2 |
| `iadj_kw` | `string` | `varchar` |  |  | Inventory Updated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iadt` | `timestamp` | `timestamp_ntz` |  |  | Inventory Adjustment Date |
| `cons` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `cons_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `pkdf` | `string` | `varchar` |  |  | Package Definition. Max length: 6 |
| `sppk` | `integer` | `int` |  |  | Specific Packaging. Allowed values: 1, 2 |
| `sppk_kw` | `string` | `varchar` |  |  | Specific Packaging (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `serp` | `integer` | `int` |  |  | Stock Point Details Present. Allowed values: 1, 2 |
| `serp_kw` | `string` | `varchar` |  |  | Stock Point Details Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ttyp` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cinv` | `integer` | `int` |  |  | Obsolete |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `itrd` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `ilgd` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `ssqd` | `integer` | `int` |  |  | Sequence Detail |
| `hupr` | `integer` | `int` |  |  | Handling Unit(s) Present. Allowed values: 0, 1, 2 |
| `hupr_kw` | `string` | `varchar` |  |  | Handling Unit(s) Present (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `casi` | `string` | `varchar` |  |  | Extra Intrastat Info. Max length: 3 |
| `pmsk` | `string` | `varchar` |  |  | Procedure Mask. Max length: 20 |
| `acti__en_us` | `string` | `varchar` |  | `not_null` | (required) Activity - base language. Max length: 20 |
| `manl` | `integer` | `int` |  |  | Manual. Allowed values: 1, 2 |
| `manl_kw` | `string` | `varchar` |  |  | Manual (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `csvl` | `float` | `float` |  |  | Customs Value |
| `csvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `nspb` | `integer` | `int` |  |  | Not Shipped Inventory Paid by. Allowed values: 10, 20, 30, 90 |
| `nspb_kw` | `string` | `varchar` |  |  | Not Shipped Inventory Paid by (keyword). Allowed values: whdpby.terms.and.cond, whdpby.own.comp, whdpby.owner, whdpby.not.appl |
| `ctyo` | `string` | `varchar` |  |  | Country of Origin. Max length: 3 |
| `addc` | `integer` | `int` |  |  | Additional Cost. Allowed values: 0, 1, 2 |
| `addc_kw` | `string` | `varchar` |  |  | Additional Cost (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `amnt` | `float` | `float` |  |  | Amount |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `dlpt` | `string` | `varchar` |  |  | Delivery Point. Max length: 9 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aclr` | `integer` | `int` |  |  | Additional Costs. Allowed values: 10, 20, 30, 40 |
| `aclr_kw` | `string` | `varchar` |  |  | Additional Costs (keyword). Allowed values: whadd.costs.calculated, whadd.costs.modified, whadd.costs.no, whadd.costs.not.applicable |
| `acfl` | `integer` | `int` |  |  | Cost for Line |
| `circ__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item Revision - base language. Max length: 35 |
| `dphu` | `integer` | `int` |  |  | Dismantle Picked Handling Unit. Allowed values: 1, 2 |
| `dphu_kw` | `string` | `varchar` |  |  | Dismantle Picked Handling Unit (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sarq` | `integer` | `int` |  |  | Source Acceptance Required. Allowed values: 1, 2 |
| `sarq_kw` | `string` | `varchar` |  |  | Source Acceptance Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aqty` | `float` | `float` |  |  | Source Accepted Quantity |
| `darq` | `integer` | `int` |  |  | Destination Acceptance Required. Allowed values: 1, 2 |
| `darq_kw` | `string` | `varchar` |  |  | Destination Acceptance Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rqty` | `float` | `float` |  |  | Destination Rejected Quantity |
| `frsm` | `integer` | `int` |  |  | Freeze Mandatory. Allowed values: 1, 2 |
| `frsm_kw` | `string` | `varchar` |  |  | Freeze Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shvl` | `integer` | `int` |  |  | Shipment Validation. Allowed values: 1, 2 |
| `shvl_kw` | `string` | `varchar` |  |  | Shipment Validation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psde` | `integer` | `int` |  |  | Print Shipping Documents by External Application. Allowed values: 1, 2 |
| `psde_kw` | `string` | `varchar` |  |  | Print Shipping Documents by External Application (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hubc` | `integer` | `int` |  |  | Handling Unit Based Confirmation. Allowed values: 1, 2 |
| `hubc_kw` | `string` | `varchar` |  |  | Handling Unit Based Confirmation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fisc` | `integer` | `int` |  |  | Finalize Order Line after Shipment Cancelation. Allowed values: 1, 2 |
| `fisc_kw` | `string` | `varchar` |  |  | Finalize Order Line after Shipment Cancelation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tcst` | `integer` | `int` |  |  | Document Compliance Status. Allowed values: 10, 20, 30, 40, 50 |
| `tcst_kw` | `string` | `varchar` |  |  | Document Compliance Status (keyword). Allowed values: tcgtc.tcst.to.be.validated, tcgtc.tcst.validating, tcgtc.tcst.valid.error, tcgtc.tcst.validated, tcgtc.tcst.not.appl |
| `exfr` | `integer` | `int` |  |  | Expedited. Allowed values: 1, 2 |
| `exfr_kw` | `string` | `varchar` |  |  | Expedited (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `atxt` | `integer` | `int` |  |  | Source Acceptance Text |
| `dtxt` | `integer` | `int` |  |  | Destination Acceptance Text |
| `text` | `integer` | `int` |  |  | Shipment Line Text |
| `shpm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh430 Shipments |
| `item_clot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whltc100 Lots |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `recd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `worn_wpon_pwsq_ssqd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh420 Shipping Sequence |
| `cwar_loca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `huid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd530 Handling Units |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `csvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ctyo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `stad_dlpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom134 Delivery Points |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `atxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `dtxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `conv_buar` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Area |
| `conv_buln` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Length |
| `conv_buvl` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Volume |
| `conv_buwg` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Weight |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Amount in Reporting Currency 2 |
| `amnt_rfrc` | `float` | `float` |  |  | CC: Amount in Reference Currency |
| `amnt_dtwc` | `float` | `float` |  |  | CC: Amount in Data Warehouse Currency |
| `sham_lclc` | `float` | `float` |  |  | CC: Shipped Amount in Local Currency |
| `sham_rpc1` | `float` | `float` |  |  | CC: Shipped Amount in Reporting Currency 1 |
| `sham_rpc2` | `float` | `float` |  |  | CC: Shipped Amount in Reporting Currency 2 |
| `sham_rfrc` | `float` | `float` |  |  | CC: Shipped Amount in Reference Currency |
| `sham_dtwc` | `float` | `float` |  |  | CC: Shipped Amount in Data Warehouse Currency |
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

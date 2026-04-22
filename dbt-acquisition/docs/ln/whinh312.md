# whinh312

LN whinh312 Receipt Lines table, Generated 2026-04-10T19:42:49Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh312` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `rcno`, `rcln` |
| **Column count** | 155 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `rcno` | `string` | `varchar` | `PK` | `not_null` | (required) Receipt. Max length: 9 |
| `rcln` | `integer` | `int` | `PK` | `not_null` | (required) Receipt Line |
| `oorg` | `integer` | `int` |  |  | Order Origin. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `oorg_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `oset` | `integer` | `int` |  |  | Set |
| `pono` | `integer` | `int` |  |  | Line |
| `seqn` | `integer` | `int` |  |  | Sequence |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `itag` | `string` | `varchar` |  |  | Inventory Tag. Max length: 9 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `revi` | `string` | `varchar` |  |  | E-item Revision. Max length: 6 |
| `idat` | `timestamp` | `timestamp_ntz` |  |  | Inventory Date |
| `rcun` | `string` | `varchar` |  |  | Receipt Unit. Max length: 3 |
| `qrcr` | `float` | `float` |  |  | Received Quantity in Receipt Unit |
| `qrec` | `float` | `float` |  |  | Received Quantity in Inventory Unit |
| `qrcp` | `float` | `float` |  |  | Received Quantity in Piece Unit |
| `qapr` | `float` | `float` |  |  | Approved Quantity |
| `qapp` | `float` | `float` |  |  | Approved Quantity in Piece Unit |
| `qdes` | `float` | `float` |  |  | Destroyed Quantity |
| `qdep` | `float` | `float` |  |  | Destroyed Quantity in Piece Unit |
| `qrej` | `float` | `float` |  |  | Rejected Quantity |
| `qrpu` | `float` | `float` |  |  | Rejected Quantity in Piece Unit |
| `qrsc` | `float` | `float` |  |  | Scrapped Quantity |
| `qrsp` | `float` | `float` |  |  | Scrapped Quantity in Piece Unit |
| `qadv` | `float` | `float` |  |  | Advised Quantity |
| `qadp` | `float` | `float` |  |  | Advised Quantity in Piece Unit |
| `qput` | `float` | `float` |  |  | Put Away Quantity |
| `qpup` | `float` | `float` |  |  | Put Away Quantity in Piece Unit |
| `qadi` | `float` | `float` |  |  | Advised from Inspection Quantity |
| `qapu` | `float` | `float` |  |  | Advised from Inspection Quantity in Piece Unit |
| `qpui` | `float` | `float` |  |  | Put Away from Inspection Quantity |
| `qppu` | `float` | `float` |  |  | Put Away from Inspection Quantity in Piece Unit |
| `psno__en_us` | `string` | `varchar` |  | `not_null` | (required) Packing Slip - base language. Max length: 30 |
| `psqr` | `float` | `float` |  |  | Packing Slip Quantity in Receipt Unit |
| `psqp` | `float` | `float` |  |  | Packing Slip Quantity in Piece Unit |
| `psqu` | `float` | `float` |  |  | Packing Slip Quantity |
| `psun` | `string` | `varchar` |  |  | Packing Slip Unit. Max length: 3 |
| `cwar` | `string` | `varchar` |  |  | Receiving Warehouse. Max length: 6 |
| `rclo` | `string` | `varchar` |  |  | Receiving Location. Max length: 10 |
| `dslo` | `string` | `varchar` |  |  | Destination Location. Max length: 10 |
| `pkdf` | `string` | `varchar` |  |  | Package Definition. Max length: 6 |
| `pmsk` | `string` | `varchar` |  |  | Procedure Mask. Max length: 20 |
| `acti__en_us` | `string` | `varchar` |  | `not_null` | (required) Activity - base language. Max length: 20 |
| `lsta` | `integer` | `int` |  |  | Line Status. Allowed values: 10, 20, 25, 30 |
| `lsta_kw` | `string` | `varchar` |  |  | Line Status (keyword). Allowed values: whinh.lstc.unexpected, whinh.lstc.received, whinh.lstc.confirming, whinh.lstc.confirmed |
| `cmpl` | `integer` | `int` |  |  | Complete. Allowed values: 1, 2 |
| `cmpl_kw` | `string` | `varchar` |  |  | Complete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fire` | `integer` | `int` |  |  | Final Receipt. Allowed values: 1, 2 |
| `fire_kw` | `string` | `varchar` |  |  | Final Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `conf` | `integer` | `int` |  |  | Confirmed. Allowed values: 1, 2 |
| `conf_kw` | `string` | `varchar` |  |  | Confirmed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `insp` | `integer` | `int` |  |  | Inspection. Allowed values: 1, 2 |
| `insp_kw` | `string` | `varchar` |  |  | Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdck` | `integer` | `int` |  |  | Cross-docking. Allowed values: 1, 2 |
| `cdck_kw` | `string` | `varchar` |  |  | Cross-docking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdcr` | `integer` | `int` |  |  | Generate Cross-dock Order Lines when Confirming Receipt. Allowed values: 1, 2 |
| `cdcr_kw` | `string` | `varchar` |  |  | Generate Cross-dock Order Lines when Confirming Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dmse` | `integer` | `int` |  |  | Direct Material Supply Execution. Allowed values: 1, 2, 10 |
| `dmse_kw` | `string` | `varchar` |  |  | Direct Material Supply Execution (keyword). Allowed values: whinh.dmse.to.be.processed, whinh.dmse.processed, whinh.dmse.not.appl |
| `qdms` | `float` | `float` |  |  | Direct Material Supply Quantity Executed |
| `arej` | `integer` | `int` |  |  | From Quarantine. Allowed values: 1, 2 |
| `arej_kw` | `string` | `varchar` |  |  | From Quarantine (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ardt` | `timestamp` | `timestamp_ntz` |  |  | Actual Receipt Date |
| `shdt` | `timestamp` | `timestamp_ntz` |  |  | Shipping Date |
| `trdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Date |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 30 |
| `ksig` | `string` | `varchar` |  |  | Kanban Signal ID. Max length: 20 |
| `ltbp` | `string` | `varchar` |  |  | Business Partner's Lot Code. Max length: 30 |
| `exhu` | `string` | `varchar` |  |  | External Handling Unit. Max length: 25 |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `hupr` | `integer` | `int` |  |  | Handling Unit(s) Present. Allowed values: 1, 2 |
| `hupr_kw` | `string` | `varchar` |  |  | Handling Unit(s) Present (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shid` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `shsq` | `integer` | `int` |  |  | Sequence |
| `load` | `string` | `varchar` |  |  | Load. Max length: 9 |
| `shpm` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `wght` | `float` | `float` |  |  | Weight |
| `wtun` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `cpro__en_us` | `string` | `varchar` |  | `not_null` | (required) Carrier Tracking Number - base language. Max length: 30 |
| `blck` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `blck_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `resn` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `logn` | `string` | `varchar` |  |  | User. Max length: 16 |
| `casi` | `string` | `varchar` |  |  | Extra Intrastat Info. Max length: 3 |
| `tsrl` | `string` | `varchar` |  |  | Obsolete. Max length: 30 |
| `tlot` | `string` | `varchar` |  |  | Obsolete. Max length: 20 |
| `exrr` | `string` | `varchar` |  |  | External Receipt Reference. Max length: 20 |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `disp` | `integer` | `int` |  |  | Quarantine Inventory Payable to Supplier. Allowed values: 1, 2 |
| `disp_kw` | `string` | `varchar` |  |  | Quarantine Inventory Payable to Supplier (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tcst` | `integer` | `int` |  |  | Document Compliance Status. Allowed values: 10, 20, 30, 40, 50 |
| `tcst_kw` | `string` | `varchar` |  |  | Document Compliance Status (keyword). Allowed values: tcgtc.tcst.to.be.validated, tcgtc.tcst.validating, tcgtc.tcst.valid.error, tcgtc.tcst.validated, tcgtc.tcst.not.appl |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `csvl` | `float` | `float` |  |  | Customs Value |
| `csvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `rtcf` | `string` | `varchar` |  |  | Return Certificate. Max length: 19 |
| `circ__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item Revision - base language. Max length: 35 |
| `imre__en_us` | `string` | `varchar` |  | `not_null` | (required) Import Reference - base language. Max length: 30 |
| `irdt` | `timestamp` | `timestamp_ntz` |  |  | Import Reference Date |
| `imrk__en_us` | `string` | `varchar` |  | `not_null` | (required) Import Reference Key - base language. Max length: 30 |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `txtn` | `integer` | `int` |  |  | Receipt Line Text |
| `itxt` | `integer` | `int` |  |  | Inspection Text |
| `oorg_orno_pono_seqn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh210 Inbound Order Lines |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `item_pkdf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd430 Package Definitions by Item |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `rcun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `psun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cwar_rclo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_dslo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `pkdf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd410 Package Definitions |
| `huid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd530 Handling Units |
| `wtun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `resn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `csvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `txtn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `itxt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `conv_buar` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Area |
| `conv_buln` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Length |
| `conv_buvl` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Volume |
| `conv_buwg` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Weight |
| `rcam_lclc` | `float` | `float` |  |  | CC: Received Amount in Local Currency |
| `rcam_rpc1` | `float` | `float` |  |  | CC: Received Amount in Reporting Currency 1 |
| `rcam_rpc2` | `float` | `float` |  |  | CC: Received Amount in Reporting Currency 2 |
| `rcam_rfrc` | `float` | `float` |  |  | CC: Received Amount in Reference Currency |
| `rcam_dtwc` | `float` | `float` |  |  | CC: Received Amount in Data Warehouse Currency |
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

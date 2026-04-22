# whinh210

LN whinh210 Inbound Order Lines table, Generated 2026-04-10T19:42:47Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh210` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `oorg`, `orno`, `pono`, `seqn` |
| **Column count** | 178 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `oorg` | `integer` | `int` | `PK` | `not_null` | (required) Order Origin. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `oorg_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `oset` | `integer` | `int` |  |  | Set |
| `comp` | `integer` | `int` |  |  | Ship-to Company |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Receiving Warehouse. Max length: 6 |
| `acvt` | `integer` | `int` |  |  | Activated. Allowed values: 1, 2 |
| `acvt_kw` | `string` | `varchar` |  |  | Activated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iubw` | `integer` | `int` |  |  | In Use by WMS. Allowed values: 1, 2 |
| `iubw_kw` | `string` | `varchar` |  |  | In Use by WMS (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fire` | `integer` | `int` |  |  | Final Receipt. Allowed values: 1, 2 |
| `fire_kw` | `string` | `varchar` |  |  | Final Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `stag` | `string` | `varchar` |  |  | Supply Tag. Max length: 9 |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `lsel` | `integer` | `int` |  |  | Lot Selection. Allowed values: 1, 2, 3 |
| `lsel_kw` | `string` | `varchar` |  |  | Lot Selection (keyword). Allowed values: tclsel.any, tclsel.same, tclsel.specific |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `revi` | `string` | `varchar` |  |  | E-item Revision. Max length: 6 |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `qoro` | `float` | `float` |  |  | Ordered Quantity in Order Unit |
| `orun` | `string` | `varchar` |  |  | Order Unit. Max length: 3 |
| `qord` | `float` | `float` |  |  | Ordered Quantity |
| `qorp` | `float` | `float` |  |  | Ordered Quantity in Piece Unit |
| `qgit` | `float` | `float` |  |  | Goods in Transit |
| `qgip` | `float` | `float` |  |  | Goods in Transit in Piece Unit |
| `qiit` | `float` | `float` |  |  | Inventory in Transit |
| `qiip` | `float` | `float` |  |  | Inventory in Transit in Piece Unit |
| `qtbr` | `float` | `float` |  |  | To be Received Quantity |
| `qtbp` | `float` | `float` |  |  | To be Received Quantity in Piece Unit |
| `qrec` | `float` | `float` |  |  | Received Quantity |
| `qrep` | `float` | `float` |  |  | Received Quantity in Piece Unit |
| `qorc` | `float` | `float` |  |  | Open Received Quantity |
| `qopu` | `float` | `float` |  |  | Open Received Quantity in Piece Unit |
| `qadv` | `float` | `float` |  |  | Advised Quantity |
| `qadp` | `float` | `float` |  |  | Advised Quantity in Piece Unit |
| `qapr` | `float` | `float` |  |  | Approved Quantity |
| `qapp` | `float` | `float` |  |  | Approved Quantity in Piece Unit |
| `qdes` | `float` | `float` |  |  | Destroyed Quantity |
| `qdep` | `float` | `float` |  |  | Destroyed Quantity in Piece Unit |
| `qrej` | `float` | `float` |  |  | Rejected Quantity |
| `qrpu` | `float` | `float` |  |  | Rejected Quantity in Piece Unit |
| `qrsc` | `float` | `float` |  |  | Scrapped Quantity |
| `qrsp` | `float` | `float` |  |  | Scrapped Quantity in Piece Unit |
| `qput` | `float` | `float` |  |  | Put Away Quantity |
| `qpup` | `float` | `float` |  |  | Put Away Quantity in Piece Unit |
| `qadi` | `float` | `float` |  |  | Advised from Inspection Quantity |
| `qapu` | `float` | `float` |  |  | Advised from Inspection Quantity in Piece Unit |
| `qpui` | `float` | `float` |  |  | Put Away from Inspection Quantity |
| `qppu` | `float` | `float` |  |  | Put Away from Inspection Quantity in Piece Unit |
| `psqu` | `float` | `float` |  |  | Packing Slip Quantity in Inventory Unit |
| `psqp` | `float` | `float` |  |  | Packing Slip Quantity in Piece Unit |
| `hstq` | `integer` | `int` |  |  | Hard Stop on Quantity. Allowed values: 1, 2, 3 |
| `hstq_kw` | `string` | `varchar` |  |  | Hard Stop on Quantity (keyword). Allowed values: tchstp.no, tchstp.warn, tchstp.block |
| `hstd` | `integer` | `int` |  |  | Hard Stop on Time. Allowed values: 1, 2, 3 |
| `hstd_kw` | `string` | `varchar` |  |  | Hard Stop on Time (keyword). Allowed values: tchstp.no, tchstp.warn, tchstp.block |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `inup` | `integer` | `int` |  |  | Inventory Handling. Allowed values: 1, 2 |
| `inup_kw` | `string` | `varchar` |  |  | Inventory Handling (keyword). Allowed values: tcinup.by.main.item, tcinup.by.component |
| `bflh` | `integer` | `int` |  |  | Backflush Order. Allowed values: 1, 2 |
| `bflh_kw` | `string` | `varchar` |  |  | Backflush Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `blck` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `blck_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdck` | `integer` | `int` |  |  | Cross-docking. Allowed values: 1, 2 |
| `cdck_kw` | `string` | `varchar` |  |  | Cross-docking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `kbpr` | `integer` | `int` |  |  | Kanban. Allowed values: 1, 2 |
| `kbpr_kw` | `string` | `varchar` |  |  | Kanban (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `insp` | `integer` | `int` |  |  | Inspection. Allowed values: 1, 2 |
| `insp_kw` | `string` | `varchar` |  |  | Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rclo` | `string` | `varchar` |  |  | Receiving Location. Max length: 10 |
| `dslo` | `string` | `varchar` |  |  | Destination Location. Max length: 10 |
| `pkdf` | `string` | `varchar` |  |  | Package Definition. Max length: 6 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `casi` | `string` | `varchar` |  |  | Extra Intrastat Info. Max length: 3 |
| `gefo` | `integer` | `int` |  |  | Generate Freight Order from Warehousing. Allowed values: 1, 2 |
| `gefo_kw` | `string` | `varchar` |  |  | Generate Freight Order from Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fmor` | `string` | `varchar` |  |  | Freight Order. Max length: 9 |
| `fmln` | `integer` | `int` |  |  | Freight Order Line |
| `uwop` | `integer` | `int` |  |  | Use Warehouse Order Price. Allowed values: 1, 2 |
| `uwop_kw` | `string` | `varchar` |  |  | Use Warehouse Order Price (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `orpr` | `float` | `float` |  |  | Order Price |
| `ocur` | `string` | `varchar` |  |  | Order Price Currency. Max length: 3 |
| `csvl` | `float` | `float` |  |  | Customs Value |
| `csvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `rorg` | `integer` | `int` |  |  | Originating Order Origin for Return. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `rorg_kw` | `string` | `varchar` |  |  | Originating Order Origin for Return (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `rorn` | `string` | `varchar` |  |  | Originating Order for Return. Max length: 9 |
| `rpon` | `integer` | `int` |  |  | Originating Order Line for Return |
| `rseq` | `integer` | `int` |  |  | Originating Order Sequence for Return |
| `paym` | `integer` | `int` |  |  | Payment. Allowed values: 10, 20, 30, 90 |
| `paym_kw` | `string` | `varchar` |  |  | Payment (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `ipay` | `integer` | `int` |  |  | Internal Payment. Allowed values: 10, 20, 30, 90 |
| `ipay_kw` | `string` | `varchar` |  |  | Internal Payment (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `owns` | `integer` | `int` |  |  | Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `fprj` | `string` | `varchar` |  |  | From Project. Max length: 9 |
| `fspa` | `string` | `varchar` |  |  | From Element. Max length: 16 |
| `fact` | `string` | `varchar` |  |  | From Activity. Max length: 16 |
| `fstl` | `string` | `varchar` |  |  | From Extension. Max length: 4 |
| `fcco` | `string` | `varchar` |  |  | From Cost Component. Max length: 8 |
| `tprj` | `string` | `varchar` |  |  | To Project. Max length: 9 |
| `tspa` | `string` | `varchar` |  |  | To Element. Max length: 16 |
| `tact` | `string` | `varchar` |  |  | To Activity. Max length: 16 |
| `tstl` | `string` | `varchar` |  |  | To Extension. Max length: 4 |
| `tcco` | `string` | `varchar` |  |  | To Cost Component. Max length: 8 |
| `prjp` | `integer` | `int` |  |  | Peg Distribution. Allowed values: 1, 2 |
| `prjp_kw` | `string` | `varchar` |  |  | Peg Distribution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 30 |
| `tlot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `tsrl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lccl` | `string` | `varchar` |  |  | Landed Costs Classification. Max length: 12 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `gnld` | `string` | `varchar` |  |  | General Ledger. Max length: 22 |
| `wmss` | `integer` | `int` |  |  | WMS Status. Allowed values: 10, 30, 40, 50, 100 |
| `wmss_kw` | `string` | `varchar` |  |  | WMS Status (keyword). Allowed values: tcwmss.expected, tcwmss.sent, tcwmss.in.process, tcwmss.closed, tcwmss.not.appl |
| `bpit__en_us` | `string` | `varchar` |  | `not_null` | (required) Business Partner Item - base language. Max length: 35 |
| `pmsk` | `string` | `varchar` |  |  | Procedure Mask. Max length: 20 |
| `acti__en_us` | `string` | `varchar` |  | `not_null` | (required) Activity - base language. Max length: 20 |
| `lsta` | `integer` | `int` |  |  | Line Status. Allowed values: 2, 5, 7, 10, 15, 20, 25, 35 |
| `lsta_kw` | `string` | `varchar` |  |  | Line Status (keyword). Allowed values: whinh.lsta.planned, whinh.lsta.open, whinh.lsta.receipt.open, whinh.lsta.received, whinh.lsta.to.be.inspected, whinh.lsta.inspected, whinh.lsta.adviced, whinh.lsta.put.away |
| `fshn` | `string` | `varchar` |  |  | First Shipment. Max length: 9 |
| `fshl` | `integer` | `int` |  |  | Line |
| `txtn` | `integer` | `int` |  |  | Inbound Text |
| `oorg_orno_oset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh200 Warehousing Orders |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
| `item_pkdf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd430 Package Definitions by Item |
| `item_clot_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whltc100 Lots |
| `item_atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd466 Item Warehousing - Attribute Sets |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd464 Attribute Sets |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `orun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `pkdf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd410 Package Definitions |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `ocur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `csvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `fprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `fcco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `tprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `tcco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `lccl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tclct020 Landed Costs Classifications |
| `txtn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `conv_buar` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Area |
| `conv_buln` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Length |
| `conv_buvl` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Volume |
| `conv_buwg` | `float` | `float` |  |  | CC: Conversion Factor from Inventory Unit to Base Unit Weight |
| `amnt_lclc` | `float` | `float` |  |  | CC: Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Amount in Reporting Currency 2 |
| `amnt_rfrc` | `float` | `float` |  |  | CC: Amount in Reference Currency |
| `amnt_dtwc` | `float` | `float` |  |  | CC: Amount in Data Warehouse Currency |
| `date_ardt` | `timestamp` | `timestamp_ntz` |  |  | CC: Actual Receipt Date |
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

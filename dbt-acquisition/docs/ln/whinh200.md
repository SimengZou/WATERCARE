# whinh200

LN whinh200 Warehousing Orders table, Generated 2026-04-10T19:42:47Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `oorg`, `orno`, `oset` |
| **Column count** | 152 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `oorg` | `integer` | `int` | `PK` | `not_null` | (required) Order Origin. Allowed values: 1, 2, 3, 11, 12, 31, 32, 33, 34, 40, 41, 50, 51, 53, 55, 56, 58, 60, 61, 71, 72, 75, 76, 78, 80, 81, 82, 90, 100 |
| `oorg_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: whinh.oorg.sales, whinh.oorg.sales.sched, whinh.oorg.sales.man, whinh.oorg.service, whinh.oorg.service.man, whinh.oorg.maint.sales, whinh.oorg.maint.sales.man, whinh.oorg.maint.work, whinh.oorg.maint.work.man, whinh.oorg.customer.claim, whinh.oorg.supplier.claim, whinh.oorg.production, whinh.oorg.production.man, whinh.oorg.product.sched, whinh.oorg.product.asc, whinh.oorg.product.asc.man, whinh.oorg.product.kanban, whinh.oorg.transformation, whinh.oorg.assembly, whinh.oorg.transfer, whinh.oorg.transfer.man, whinh.oorg.project, whinh.oorg.project.man, whinh.oorg.proj.contract, whinh.oorg.purchase, whinh.oorg.purchase.sched, whinh.oorg.purchase.man, whinh.oorg.enterprise.plan, whinh.oorg.not.appl |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Order. Max length: 9 |
| `oset` | `integer` | `int` | `PK` | `not_null` | (required) Set |
| `ittp` | `integer` | `int` |  |  | Inventory Transaction Type. Allowed values: 1, 2, 3, 5 |
| `ittp_kw` | `string` | `varchar` |  |  | Inventory Transaction Type (keyword). Allowed values: whinh.ittp.receipt, whinh.ittp.issue, whinh.ittp.transfer, whinh.ittp.wip.transfer |
| `seri` | `string` | `varchar` |  |  | Series. Max length: 8 |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `sfcp` | `integer` | `int` |  |  | Ship-from Company |
| `sfty` | `integer` | `int` |  |  | Ship-from Type. Allowed values: 1, 2, 3, 4, 10 |
| `sfty_kw` | `string` | `varchar` |  |  | Ship-from Type (keyword). Allowed values: tctyps.warehouse, tctyps.partner, tctyps.project, tctyps.work.center, tctyps.not.appl |
| `sfco` | `string` | `varchar` |  |  | Ship-from Code. Max length: 9 |
| `sfsi` | `string` | `varchar` |  |  | Ship-from Site. Max length: 9 |
| `stcp` | `integer` | `int` |  |  | Ship-to Company |
| `stty` | `integer` | `int` |  |  | Ship-to Type. Allowed values: 1, 2, 3, 4, 10 |
| `stty_kw` | `string` | `varchar` |  |  | Ship-to Type (keyword). Allowed values: tctyps.warehouse, tctyps.partner, tctyps.project, tctyps.work.center, tctyps.not.appl |
| `stco` | `string` | `varchar` |  |  | Ship-to Code. Max length: 9 |
| `stsi` | `string` | `varchar` |  |  | Ship-to Site. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `rttr` | `integer` | `int` |  |  | Rental Product Transfer. Allowed values: 0, 5, 10, 15, 17, 20 |
| `rttr_kw` | `string` | `varchar` |  |  | Rental Product Transfer (keyword). Allowed values: , whinh.rttr.pr.to.rtpr, whinh.rttr.rtpr.to.pr, whinh.rttr.rtpr.to.pr.recl, whinh.rttr.rown.change, whinh.rttr.not.applicable |
| `sfit` | `string` | `varchar` |  |  | From Item. Max length: 47 |
| `stit` | `string` | `varchar` |  |  | Into Item. Max length: 47 |
| `sfrv` | `string` | `varchar` |  |  | From Revision. Max length: 6 |
| `strv` | `string` | `varchar` |  |  | Into Revision. Max length: 6 |
| `sfsr` | `string` | `varchar` |  |  | From Serial Number. Max length: 30 |
| `stsr` | `string` | `varchar` |  |  | Into Serial Number. Max length: 30 |
| `sflt` | `string` | `varchar` |  |  | From Lot. Max length: 20 |
| `stlt` | `string` | `varchar` |  |  | Into Lot. Max length: 20 |
| `sfrc` | `integer` | `int` |  |  | From Rental Owner Company |
| `sfro` | `string` | `varchar` |  |  | From Rental Owner. Max length: 6 |
| `strc` | `integer` | `int` |  |  | Into Rental Owner Company |
| `stro` | `string` | `varchar` |  |  | Into Rental Owner. Max length: 6 |
| `sflo` | `string` | `varchar` |  |  | Ship-from Location. Max length: 10 |
| `stlo` | `string` | `varchar` |  |  | Ship-to Location. Max length: 10 |
| `otyp` | `string` | `varchar` |  |  | Warehousing Order Type. Max length: 3 |
| `depc` | `integer` | `int` |  |  | Office Company |
| `wdep` | `string` | `varchar` |  |  | Office. Max length: 6 |
| `blor` | `integer` | `int` |  |  | Blanket Order. Allowed values: 1, 2 |
| `blor_kw` | `string` | `varchar` |  |  | Blanket Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rtrn` | `integer` | `int` |  |  | Return Order. Allowed values: 1, 2 |
| `rtrn_kw` | `string` | `varchar` |  |  | Return Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cons` | `integer` | `int` |  |  | Consignment. Allowed values: 1, 2 |
| `cons_kw` | `string` | `varchar` |  |  | Consignment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `subc` | `integer` | `int` |  |  | Subcontracting. Allowed values: 10, 20, 30, 40, 50, 60 |
| `subc_kw` | `string` | `varchar` |  |  | Subcontracting (keyword). Allowed values: tcsubc.no, tcsubc.yes, tcsubc.operation, tcsubc.item, tcsubc.material, tcsubc.wip |
| `fqua` | `integer` | `int` |  |  | From Quarantine. Allowed values: 1, 2 |
| `fqua_kw` | `string` | `varchar` |  |  | From Quarantine (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `quar` | `integer` | `int` |  |  | To Quarantine. Allowed values: 1, 2 |
| `quar_kw` | `string` | `varchar` |  |  | To Quarantine (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bflh` | `integer` | `int` |  |  | Backflush Order. Allowed values: 1, 2 |
| `bflh_kw` | `string` | `varchar` |  |  | Backflush Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rrgd` | `integer` | `int` |  |  | Return Rejected Goods. Allowed values: 1, 2 |
| `rrgd_kw` | `string` | `varchar` |  |  | Return Rejected Goods (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rdgd` | `integer` | `int` |  |  | Return Quarantine Inventory Payable to Supplier. Allowed values: 1, 2 |
| `rdgd_kw` | `string` | `varchar` |  |  | Return Quarantine Inventory Payable to Supplier (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dmst` | `integer` | `int` |  |  | Direct Material Supply Transfer. Allowed values: 1, 2 |
| `dmst_kw` | `string` | `varchar` |  |  | Direct Material Supply Transfer (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `plbs` | `integer` | `int` |  |  | Planning by Supplier. Allowed values: 1, 2 |
| `plbs_kw` | `string` | `varchar` |  |  | Planning by Supplier (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ktor` | `integer` | `int` |  |  | Order for Kit. Allowed values: 1, 2 |
| `ktor_kw` | `string` | `varchar` |  |  | Order for Kit (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `carr` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `cbin` | `integer` | `int` |  |  | Carrier/LSP Binding. Allowed values: 1, 2 |
| `cbin_kw` | `string` | `varchar` |  |  | Carrier/LSP Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `motv` | `string` | `varchar` |  |  | Motive of Transport. Max length: 6 |
| `delc` | `string` | `varchar` |  |  | Delivery Code. Max length: 6 |
| `serv` | `string` | `varchar` |  |  | Freight Service Level. Max length: 3 |
| `pddt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `mint` | `float` | `float` |  |  | Minimum Quantity Tolerance |
| `maxt` | `float` | `float` |  |  | Maximum Quantity Tolerance |
| `mind` | `integer` | `int` |  |  | Minimum Time Tolerance in Days |
| `maxd` | `integer` | `int` |  |  | Maximum Time Tolerance in Days |
| `scon` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `scon_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tcscon.none, tcscon.sc, tcscon.rc, tcscon.oc, tcscon.sca, tcscon.kc, tcscon.not.applicable |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `grid` | `string` | `varchar` |  |  | Order Group. Max length: 9 |
| `setn` | `integer` | `int` |  |  | Shipping Set |
| `info__en_us` | `string` | `varchar` |  | `not_null` | (required) Information - base language. Max length: 30 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 30 |
| `refg__en_us` | `string` | `varchar` |  | `not_null` | (required) Grouping Reference - base language. Max length: 30 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `isit` | `integer` | `int` |  |  | Assign Item Surcharges for Item Transfer. Allowed values: 10, 20, 30, 40, 100 |
| `isit_kw` | `string` | `varchar` |  |  | Assign Item Surcharges for Item Transfer (keyword). Allowed values: whinh.asur.issue.receipt, whinh.asur.issue, whinh.asur.receipt, whinh.asur.none, whinh.asur.not.appl |
| `rodr` | `string` | `varchar` |  |  | Related Multisite Order. Max length: 9 |
| `akit` | `string` | `varchar` |  |  | Assembly Kit. Max length: 15 |
| `ctdt` | `timestamp` | `timestamp_ntz` |  |  | Contract Date |
| `adat` | `timestamp` | `timestamp_ntz` |  |  | Assembly Date |
| `clgr` | `string` | `varchar` |  |  | List Group. Max length: 3 |
| `list` | `string` | `varchar` |  |  | Kit Definition. Max length: 47 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `qoro` | `float` | `float` |  |  | Ordered Quantity in Order Unit |
| `orun` | `string` | `varchar` |  |  | Order Unit. Max length: 3 |
| `qord` | `float` | `float` |  |  | Ordered Quantity in Inventory Unit |
| `lccl` | `string` | `varchar` |  |  | Landed Costs Classification. Max length: 12 |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `gnld` | `string` | `varchar` |  |  | General Ledger. Max length: 22 |
| `invc` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `invc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iscn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `iscn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asst` | `integer` | `int` |  |  | Assembly Status. Allowed values: 5, 10, 15, 20 |
| `asst_kw` | `string` | `varchar` |  |  | Assembly Status (keyword). Allowed values: whinh.asst.open, whinh.asst.assemble, whinh.asst.assembled, whinh.asst.put.away |
| `hsta` | `integer` | `int` |  |  | Header Status. Allowed values: 10, 20, 30, 60, 65, 70, 80, 90 |
| `hsta_kw` | `string` | `varchar` |  |  | Header Status (keyword). Allowed values: whinh.hsta.planned, whinh.hsta.actual, whinh.hsta.modified, whinh.hsta.received, whinh.hsta.put.away, whinh.hsta.shipped, whinh.hsta.canceled, whinh.hsta.not.appl |
| `mrtd` | `integer` | `int` |  |  | Multiple Recipients in Transportation Document. Allowed values: 1, 2 |
| `mrtd_kw` | `string` | `varchar` |  |  | Multiple Recipients in Transportation Document (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Header Text |
| `txtb` | `integer` | `int` |  |  | Footer Text |
| `txtk` | `integer` | `int` |  |  | Kit Text |
| `sfsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `sfit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `stit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `sfrc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `sfro_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `strc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `stro_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `otyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh010 Warehousing Order Types |
| `depc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `carr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `motv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `delc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `serv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs075 Freight Service Levels |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `akit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd450 Assembly Kit |
| `clgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd301 List Groups |
| `list_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `orun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `lccl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tclct020 Landed Costs Classifications |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

# tppss600

LN tppss600 Planned Material Transactions table, Generated 2026-04-10T19:42:20Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppss600` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `item`, `date`, `sern` |
| **Column count** | 84 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `date` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Planned Delivery Date |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `phit` | `string` | `varchar` |  |  | Phantom Item. Max length: 47 |
| `efph` | `integer` | `int` |  |  | Effectivity Unit Phantom |
| `quan` | `float` | `float` |  |  | Planned Quantity |
| `kotr` | `integer` | `int` |  |  | Transaction Type. Allowed values: 1, 2 |
| `kotr_kw` | `string` | `varchar` |  |  | Transaction Type (keyword). Allowed values: tckotr.receipt, tckotr.requirement |
| `koor` | `integer` | `int` |  |  | Order Type. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `koor_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: , tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `orno` | `string` | `varchar` |  |  | Order Number. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Position No. |
| `sqnb` | `integer` | `int` |  |  | Sequence Number |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `bsqn` | `integer` | `int` |  |  | Budget Line Number |
| `dsit` | `string` | `varchar` |  |  | Site of Project Warehouse. Max length: 9 |
| `dwar` | `string` | `varchar` |  |  | Project Warehouse. Max length: 6 |
| `aitm` | `string` | `varchar` |  |  | Item in Warehousing. Max length: 47 |
| `effw` | `integer` | `int` |  |  | Effectivity Unit in Warehousing |
| `buyr` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `plid` | `string` | `varchar` |  |  | Planner. Max length: 9 |
| `deli` | `integer` | `int` |  |  | Deliverable. Allowed values: 0, 1, 2 |
| `deli_kw` | `string` | `varchar` |  |  | Deliverable (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `rtdl` | `integer` | `int` |  |  | Return Deliverable. Allowed values: 1, 2 |
| `rtdl_kw` | `string` | `varchar` |  |  | Return Deliverable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `tprj` | `string` | `varchar` |  |  | To Project. Max length: 9 |
| `tspa` | `string` | `varchar` |  |  | To Element. Max length: 16 |
| `tpla` | `string` | `varchar` |  |  | To Plan. Max length: 3 |
| `tact` | `string` | `varchar` |  |  | To Activity. Max length: 16 |
| `tstl` | `string` | `varchar` |  |  | To Extension. Max length: 4 |
| `tcco` | `string` | `varchar` |  |  | To Cost Component. Max length: 8 |
| `tsit` | `string` | `varchar` |  |  | Site of To Warehouse. Max length: 9 |
| `twar` | `string` | `varchar` |  |  | To Warehouse. Max length: 6 |
| `carr` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `serv` | `string` | `varchar` |  |  | Freight Service Level. Max length: 3 |
| `crbn` | `integer` | `int` |  |  | Carrier / LSP Binding. Allowed values: 0, 1, 2 |
| `crbn_kw` | `string` | `varchar` |  |  | Carrier / LSP Binding (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm005 Item Project Data |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `phit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `efph_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `dsit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `dwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `aitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm005 Item Project Data |
| `effw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `buyr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `plid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `tsit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `twar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `carr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `serv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs075 Freight Service Levels |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
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

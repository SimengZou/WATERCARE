# tpppc211

LN tpppc211 Material Costs table, Generated 2026-04-10T19:42:07Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc211` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `item`, `sern` |
| **Column count** | 114 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `phit` | `string` | `varchar` |  |  | Phantom Item. Max length: 47 |
| `efph` | `integer` | `int` |  |  | Effectivity Unit Phantom |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `quan` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `rtcc_1` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtcc_2` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtcc_3` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `cohd_1` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `cohd_2` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `cohd_3` | `float` | `float` |  |  | Cost Amount in Home Currency (Debit) |
| `curc` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `pris` | `float` | `float` |  |  | Sales Price |
| `amos` | `float` | `float` |  |  | Sales Amount |
| `rtcs_1` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtcs_2` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtcs_3` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtfs_1` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_2` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_3` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `curs` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `cdoc__en_us` | `string` | `varchar` |  | `not_null` | (required) Document - base language. Max length: 10 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `cptc` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `year` | `integer` | `int` |  |  | Cost Control Year |
| `peri` | `integer` | `int` |  |  | Cost Control Period |
| `cptf` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `fyea` | `integer` | `int` |  |  | Fiscal Year |
| `fper` | `integer` | `int` |  |  | Fiscal Period |
| `ryea` | `integer` | `int` |  |  | Reporting Year |
| `rper` | `integer` | `int` |  |  | Reporting Period |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `koor` | `integer` | `int` |  |  | Order Type. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 68, 69, 70, 71, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 93, 94, 95, 96, 97, 98, 99, 100 |
| `koor_kw` | `string` | `varchar` |  |  | Order Type (keyword). Allowed values: , tckoor.act.sfc, tckoor.act.pur, tckoor.act.sls, tckoor.cp.sfc, tckoor.cp.pur, tckoor.act.pur.sched, tckoor.act.sls.sched, tckoor.act.pur.adv, tckoor.act.asc, tckoor.services.proc, tckoor.mrp.sls, tckoor.mrp.atp, tckoor.sls.quot, tckoor.pur.con, tckoor.sls.con, tckoor.wrh.order, tckoor.act.srv, tckoor.pss.pur, tckoor.pss.wrh, tckoor.serv.proc.adv, tckoor.wrh.ass, tckoor.act.trf, tckoor.act.pmg, tckoor.srv.call, tckoor.srv.planned.act, tckoor.cp.ipl, tckoor.cf.ap, tckoor.act.sfc.man, tckoor.act.pur.man, tckoor.act.sls.man, tckoor.act.srv.man, tckoor.act.trf.man, tckoor.act.srv.sls, tckoor.act.dpt.wrk, tckoor.act.srv.sls.man, tckoor.pur.rfq, tckoor.act.sls.sched.f, tckoor.act.dpt.wrk.man, tckoor.freight, tckoor.stock, tckoor.safety.stock, tckoor.act.asc.man, tckoor.act.pur.req, tckoor.epp.quote, tckoor.mps.prod, tckoor.mps.pur, tckoor.cycle.count, tckoor.adjustment, tckoor.apl.asc, tckoor.cp.rpt, tckoor.revaluation, tckoor.product.sched, tckoor.product.kanban, tckoor.project, tckoor.project.man, tckoor.enterprise.plan, tckoor.intercomp.trade, tckoor.cp.sub, tckoor.act.sub.sched, tckoor.pcs.project, tckoor.eus.change, tckoor.alloc.change, tckoor.inv.own.change, tckoor.exp.supply, tckoor.conf.supply, tckoor.bp.forecast, tckoor.aggr.demand, tckoor.bfbp.trf.pur, tckoor.bfbp.trf.sched, tckoor.stbp.trf.sls, tckoor.stbp.trf.sched, tckoor.stbp.trf.wh.man, tckoor.stbp.trf.man, tckoor.stbp.trf.wh.dis, tckoor.cp.cpt, tckoor.act.cpt, tckoor.tag.transfer, tckoor.proj.contract, tckoor.customer.claim, tckoor.supplier.claim, tckoor.quarantine, tckoor.transformation, tckoor.not.appl |
| `orno` | `string` | `varchar` |  |  | Purchase Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Position Number |
| `srnb` | `integer` | `int` |  |  | Receipt Number |
| `srni` | `integer` | `int` |  |  | Invoice Batch Number |
| `cfpo` | `integer` | `int` |  |  | Approved for Posting. Allowed values: 1, 2 |
| `cfpo_kw` | `string` | `varchar` |  |  | Approved for Posting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `potf` | `integer` | `int` |  |  | Post to LN Financials. Allowed values: 1, 2 |
| `potf_kw` | `string` | `varchar` |  |  | Post to LN Financials (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `tetb` | `integer` | `int` |  |  | Posting Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 |
| `tetb_kw` | `string` | `varchar` |  |  | Posting Type (keyword). Allowed values: tpppc.tetb.manu, tpppc.tetb.stock, tpppc.tetb.invoice, tpppc.tetb.order.invoice, tpppc.tetb.invoice.diff, tpppc.tetb.exp.tax.dir, tpppc.tetb.value.corr, tpppc.tetb.value.corr.rev, tpppc.tetb.service, tpppc.tetb.shipment.var |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `ncmp` | `integer` | `int` |  |  | Company Financial Document |
| `entu` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `ftty` | `string` | `varchar` |  |  | Transaction Type Financial Transaction. Max length: 3 |
| `fdoc` | `integer` | `int` |  |  | Document Financial Transaction |
| `flin` | `integer` | `int` |  |  | Line Number Financial Transaction |
| `fsrl` | `integer` | `int` |  |  | Sequence Number Finance |
| `ftln` | `integer` | `int` |  |  | Target Line Number |
| `loco` | `string` | `varchar` |  |  | User. Max length: 16 |
| `sttl` | `integer` | `int` |  |  | Settlement Origin. Allowed values: 1, 2 |
| `sttl_kw` | `string` | `varchar` |  |  | Settlement Origin (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rdas` | `timestamp` | `timestamp_ntz` |  |  | Rate Date (Sales) |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Fixed Currency Rate. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Fixed Currency Rate (keyword). Allowed values: , tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `guid` | `string` | `varchar` |  |  | Unique ID. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Text |
| `cdf_sref__en_us` | `string` | `varchar` |  | `not_null` | (required) SAP Reference - base language. Max length: 30 |
| `cdf_susr__en_us` | `string` | `varchar` |  | `not_null` | (required) SAP User - base language. Max length: 30 |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm005 Item Project Data |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `phit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `efph_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `curc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `curs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `cdoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppin105 Invoice Text by Document |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cptf_fyea_fper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
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

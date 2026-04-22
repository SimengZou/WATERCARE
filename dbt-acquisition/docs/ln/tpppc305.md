# tpppc305

LN tpppc305 Revenue Transactions table, Generated 2026-04-10T19:42:12Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc305` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cspa`, `cpro`, `sern` |
| **Column count** | 146 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `cpro` | `string` | `varchar` | `PK` | `not_null` | (required) Revenue Code. Max length: 8 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `amor` | `float` | `float` |  |  | Revenues |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `cdoc__en_us` | `string` | `varchar` |  | `not_null` | (required) Document - base language. Max length: 10 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `idoc` | `integer` | `int` |  |  | Financial Document Number |
| `ityp` | `string` | `varchar` |  |  | Financial Transaction Type. Max length: 3 |
| `iseq` | `integer` | `int` |  |  | Invoice Line Sequence |
| `cptf` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `fyea` | `integer` | `int` |  |  | Fiscal Year |
| `fper` | `integer` | `int` |  |  | Fiscal Period |
| `frgd` | `timestamp` | `timestamp_ntz` |  |  | Financial Registration Date |
| `cptc` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `year` | `integer` | `int` |  |  | Cost Control Year |
| `peri` | `integer` | `int` |  |  | Cost Control Period |
| `ncmp` | `integer` | `int` |  |  | Delivering Company |
| `ocmp` | `integer` | `int` |  |  | Company |
| `entu` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `potf` | `integer` | `int` |  |  | Post to Finance. Allowed values: 1, 2 |
| `potf_kw` | `string` | `varchar` |  |  | Post to Finance (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `comf` | `integer` | `int` |  |  | Reported Complete (Finance). Allowed values: 0, 1, 2, 3 |
| `comf_kw` | `string` | `varchar` |  |  | Reported Complete (Finance) (keyword). Allowed values: , tcynna.yes, tcynna.no, tcynna.not.app |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `rest` | `integer` | `int` |  |  | Revenue Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 |
| `rest_kw` | `string` | `varchar` |  |  | Revenue Type (keyword). Allowed values: tppdm.invt.advance, tppdm.invt.installment, tppdm.invt.inst.add.work, tppdm.invt.cost.plus, tppdm.invt.extension, tppdm.invt.unit.rate, tppdm.invt.holdback, tppdm.invt.delivery, tppdm.invt.prog.paymnt.req, tppdm.invt.fees.penalties, tppdm.invt.credit.note |
| `tetl` | `integer` | `int` |  |  | Posting Type. Allowed values: 1, 2, 3, 4, 10, 20 |
| `tetl_kw` | `string` | `varchar` |  |  | Posting Type (keyword). Allowed values: tpppc.tetl.manu, tpppc.tetl.invoicing, tpppc.tetl.sal.invoice.fin, tpppc.tetl.invoicing.hb, tpppc.tetl.invoice.non.rev, tpppc.tetl.rev.recognition |
| `cntp` | `integer` | `int` |  |  | Revenue Recognition Counterposting. Allowed values: 1, 2 |
| `cntp_kw` | `string` | `varchar` |  |  | Revenue Recognition Counterposting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crdf` | `integer` | `int` |  |  | Currency Rate Difference. Allowed values: 1, 2 |
| `crdf_kw` | `string` | `varchar` |  |  | Currency Rate Difference (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rsst` | `integer` | `int` |  |  | Price Fluctuation Settled. Allowed values: 1, 2, 3 |
| `rsst_kw` | `string` | `varchar` |  |  | Price Fluctuation Settled (keyword). Allowed values: tpppc.rsst.none, tpppc.rsst.no, tpppc.rsst.yes |
| `koor` | `integer` | `int` |  |  | Document Type. Allowed values: 1, 2, 3, 6, 7, 9, 10, 16, 17, 18, 19, 22, 26, 28, 31, 33, 36, 37, 38, 43, 51, 52, 55, 56, 57, 58, 59, 60, 70, 78, 80, 82, 84, 86, 88, 90, 92, 93, 95, 96, 97, 100, 200, 201, 202 |
| `koor_kw` | `string` | `varchar` |  |  | Document Type (keyword). Allowed values: tpppc.koor.act.sfc, tpppc.koor.act.pur, tpppc.koor.act.sls, tpppc.koor.act.pur.sched, tpppc.koor.act.sls.sched, tpppc.koor.act.asc, tpppc.koor.services.proc, tpppc.koor.wrh.order, tpppc.koor.act.srv, tpppc.koor.pss.pur, tpppc.koor.pss.wrh, tpppc.koor.act.trf, tpppc.koor.act.pmg, tpppc.koor.srv.call, tpppc.koor.cf.ap, tpppc.koor.act.pur.man, tpppc.koor.act.trf.man, tpppc.koor.act.srv.sls, tpppc.koor.act.dpt.wrk, tpppc.koor.freight, tpppc.koor.cycle.count, tpppc.koor.adjustment, tpppc.koor.revaluation, tpppc.koor.product.sched, tpppc.koor.product.kanban, tpppc.koor.project, tpppc.koor.project.man, tpppc.koor.enterprise.plan, tpppc.koor.inv.own.change, tpppc.koor.bfbp.trf.pur, tpppc.koor.bfbp.trf.sched, tpppc.koor.stbp.trf.sls, tpppc.koor.stbp.trf.sched, tpppc.koor.stbp.trf.wh.man, tpppc.koor.stbp.trf.man, tpppc.koor.stbp.trf.wh.dis, tpppc.koor.cp.cpt, tpppc.koor.act.cpt, tpppc.koor.proj.contract, tpppc.koor.customer.claim, tpppc.koor.supplier.claim, tpppc.koor.not.appl, tpppc.koor.srv.call.obs, tpppc.koor.srv.contract, tpppc.koor.financial.doc |
| `orno` | `string` | `varchar` |  |  | Document. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Document Line |
| `schd` | `integer` | `int` |  |  | Schedule |
| `drun` | `timestamp` | `timestamp_ntz` |  |  | Run Date |
| `serb` | `integer` | `int` |  |  | Processing Run Number |
| `cdru` | `timestamp` | `timestamp_ntz` |  |  | Run Date (Report Completed) |
| `cser` | `integer` | `int` |  |  | Run Sequence No. (Report Completed) |
| `rate_1` | `float` | `float` |  |  | Rate (Debit) |
| `rate_2` | `float` | `float` |  |  | Rate (Debit) |
| `rate_3` | `float` | `float` |  |  | Rate (Debit) |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor (Debit) |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor (Debit) |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor (Debit) |
| `amhc_1` | `float` | `float` |  |  | Revenue in Home Currency (Debit) |
| `amhc_2` | `float` | `float` |  |  | Revenue in Home Currency (Debit) |
| `amhc_3` | `float` | `float` |  |  | Revenue in Home Currency (Debit) |
| `rtcr_1` | `float` | `float` |  |  | Rate (Credit) |
| `rtcr_2` | `float` | `float` |  |  | Rate (Credit) |
| `rtcr_3` | `float` | `float` |  |  | Rate (Credit) |
| `rfcr_1` | `integer` | `int` |  |  | Rate Factor (Credit) |
| `rfcr_2` | `integer` | `int` |  |  | Rate Factor (Credit) |
| `rfcr_3` | `integer` | `int` |  |  | Rate Factor (Credit) |
| `hccr_1` | `float` | `float` |  |  | Revenue in Home Currency (Credit) |
| `hccr_2` | `float` | `float` |  |  | Revenue in Home Currency (Credit) |
| `hccr_3` | `float` | `float` |  |  | Revenue in Home Currency (Credit) |
| `nadv` | `integer` | `int` |  |  | Advance Payment |
| `nins` | `integer` | `int` |  |  | Installment |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Progress Date |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Fixed Currency Rate. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Fixed Currency Rate (keyword). Allowed values: , tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `ftty` | `string` | `varchar` |  |  | Transaction Type Financial Transaction. Max length: 3 |
| `fdoc` | `integer` | `int` |  |  | Document Financial Transaction |
| `flin` | `integer` | `int` |  |  | Line Number Financial Transaction |
| `fsrl` | `integer` | `int` |  |  | Sequence Number Finance |
| `ftln` | `integer` | `int` |  |  | Target Line Number |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cnln` | `string` | `varchar` |  |  | Contract Line. Max length: 8 |
| `guid` | `string` | `varchar` |  |  | Unique ID. Max length: 22 |
| `itgd` | `string` | `varchar` |  |  | Integration Transaction GUID. Max length: 22 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | BP Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exemption Certificate. Max length: 24 |
| `exrs` | `string` | `varchar` |  |  | Exemption Reason. Max length: 6 |
| `cstv` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `emno` | `string` | `varchar` |  |  | Employee. Max length: 9 |
| `hbdc` | `string` | `varchar` |  |  | Holdback Discount Code. Max length: 3 |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cptf_fyea_fper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `drun_serb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpppc005 Processing Runs |
| `cdru_cser_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpppc005 Processing Runs |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `exrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm801 Employee Project Data |
| `hbdc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs021 Discount Codes |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `amor_refc` | `float` | `float` |  |  | CC: Revenues in Reference Currency |
| `amor_prjc` | `float` | `float` |  |  | CC: Revenues in Project Currency |
| `amor_cntc` | `float` | `float` |  |  | CC: Revenues in Contract Currency |
| `amor_dwhc` | `float` | `float` |  |  | CC: Revenues in Data Warehouse Currency |
| `amor_homc` | `float` | `float` |  |  | CC: Revenues in Locl Currency |
| `amor_rpc1` | `float` | `float` |  |  | CC: Revenues in Reference Currency 1 |
| `amor_rpc2` | `float` | `float` |  |  | CC: Revenues in Reference Currency 2 |
| `cprj_cpro` | `string` | `varchar` |  |  | CC: Project Revenue Code. Max length: 25 |
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

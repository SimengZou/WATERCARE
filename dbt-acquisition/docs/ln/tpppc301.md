# tpppc301

LN tpppc301 Revenues table, Generated 2026-04-10T19:42:11Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc301` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cpro`, `sern` |
| **Column count** | 121 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cpro` | `string` | `varchar` | `PK` | `not_null` | (required) Revenue Code. Max length: 8 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `amor` | `float` | `float` |  |  | Revenues |
| `amhc_1` | `float` | `float` |  |  | Revenue in Home Currency (Debit) |
| `amhc_2` | `float` | `float` |  |  | Revenue in Home Currency (Debit) |
| `amhc_3` | `float` | `float` |  |  | Revenue in Home Currency (Debit) |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `cdoc__en_us` | `string` | `varchar` |  | `not_null` | (required) Document - base language. Max length: 10 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `cptc` | `string` | `varchar` |  |  | Code Period Table for Cost Control Period. Max length: 6 |
| `year` | `integer` | `int` |  |  | Cost Control Year |
| `peri` | `integer` | `int` |  |  | Cost Control Period |
| `cptf` | `string` | `varchar` |  |  | Period table for financial period. Max length: 6 |
| `fyea` | `integer` | `int` |  |  | Fiscal Year |
| `fper` | `integer` | `int` |  |  | Fiscal Period |
| `ryea` | `integer` | `int` |  |  | Reporting Year |
| `rper` | `integer` | `int` |  |  | Reporting Period |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `cfpo` | `integer` | `int` |  |  | Approved for Posting. Allowed values: 1, 2 |
| `cfpo_kw` | `string` | `varchar` |  |  | Approved for Posting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `potf` | `integer` | `int` |  |  | Post to LN Financials. Allowed values: 1, 2 |
| `potf_kw` | `string` | `varchar` |  |  | Post to LN Financials (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rest` | `integer` | `int` |  |  | Invoicing Method. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 |
| `rest_kw` | `string` | `varchar` |  |  | Invoicing Method (keyword). Allowed values: tppdm.invt.advance, tppdm.invt.installment, tppdm.invt.inst.add.work, tppdm.invt.cost.plus, tppdm.invt.extension, tppdm.invt.unit.rate, tppdm.invt.holdback, tppdm.invt.delivery, tppdm.invt.prog.paymnt.req, tppdm.invt.fees.penalties, tppdm.invt.credit.note |
| `entu` | `string` | `varchar` |  |  | Enterprise Unit. Max length: 6 |
| `ncmp` | `integer` | `int` |  |  | Delivering Company |
| `ftty` | `string` | `varchar` |  |  | Transaction Type Financial Transaction. Max length: 3 |
| `fdoc` | `integer` | `int` |  |  | Document Financial Transaction |
| `flin` | `integer` | `int` |  |  | Line Number Financial Transaction |
| `fsrl` | `integer` | `int` |  |  | Sequence Number Finance |
| `ftln` | `integer` | `int` |  |  | Target Line Number |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Rate (Debit) |
| `rate_2` | `float` | `float` |  |  | Rate (Debit) |
| `rate_3` | `float` | `float` |  |  | Rate (Debit) |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor (Debit) |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor (Debit) |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor (Debit) |
| `tetm` | `integer` | `int` |  |  | Posting Type. Allowed values: 1, 3, 5, 6, 7, 8, 9, 10, 11 |
| `tetm_kw` | `string` | `varchar` |  |  | Posting Type (keyword). Allowed values: tpppc.tetm.manu, tpppc.tetm.sal.invoice.tp, tpppc.tetm.sal.invoice.fin, tpppc.tetm.sal.inv.hb, tpppc.tetm.sal.inv.hb.rev, tpppc.tetm.sal.inv.adv, tpppc.tetm.sal.inv.adv.rev, tpppc.tetm.sal.inv.ppr, tpppc.tetm.sal.inv.ppr.rev |
| `loco` | `string` | `varchar` |  |  | User. Max length: 16 |
| `fcmp` | `integer` | `int` |  |  | Financial Company |
| `ityp` | `string` | `varchar` |  |  | Financial Transaction Type. Max length: 3 |
| `idoc` | `integer` | `int` |  |  | Financial Document number |
| `hbnr` | `integer` | `int` |  |  | Sequence Number |
| `invt` | `integer` | `int` |  |  | Invoicing Method. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 33, 34, 35, 36, 37, 38, 39, 40, 42, 50, 55, 60, 65, 67, 95, 100 |
| `invt_kw` | `string` | `varchar` |  |  | Invoicing Method (keyword). Allowed values: , tcinvt.adv.rec.request, tcinvt.installment, tcinvt.cost.plus, tcinvt.extension, tcinvt.unit.rate, tcinvt.progress, tcinvt.manual.sales, tcinvt.sales.order, tcinvt.cntr.instalment, tcinvt.material, tcinvt.labor, tcinvt.equipment, tcinvt.tool, tcinvt.travel, tcinvt.subcon, tcinvt.helpdesk, tcinvt.freight, tcinvt.interest, tcinvt.wip.transfer, tcinvt.triangular, tcinvt.hold.back, tcinvt.inst.add.costs, tcinvt.pay.from.rec, tcinvt.other, tcinvt.warehouse.order, tcinvt.schedules, tcinvt.retro, tcinvt.credit.note, tcinvt.debit.note, tcinvt.freight.order, tcinvt.sales.dir.delv, tcinvt.rebate, tcinvt.pcs.order, tcinvt.expenses, tcinvt.triangular.pur, tcinvt.contr.delivery, tcinvt.rental, tcinvt.activity, tcinvt.prog.paymnt.req, tcinvt.fees.penalties, tcinvt.adv.invoice, tcinvt.adv.pay.request, tcinvt.non.billable, tcinvt.not.app |
| `nadv` | `integer` | `int` |  |  | Advance Payment |
| `nins` | `integer` | `int` |  |  | Installment |
| `dlvr` | `integer` | `int` |  |  | Deliverable |
| `schd` | `integer` | `int` |  |  | Schedule |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Progress Date |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `fcrt` | `integer` | `int` |  |  | Fixed Currency Rate. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `fcrt_kw` | `string` | `varchar` |  |  | Fixed Currency Rate (keyword). Allowed values: , tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cnln` | `string` | `varchar` |  |  | Contract Line. Max length: 8 |
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
| `guid` | `string` | `varchar` |  |  | Unique ID. Max length: 22 |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cptc_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cptf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cono_cnln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm110 Contract Lines |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpctm100 Contracts |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
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
| `amor_homc` | `float` | `float` |  |  | CC: Revenues in Local Currency |
| `amor_rpc1` | `float` | `float` |  |  | CC: Revenues in Reporting Currency 1 |
| `amor_rpc2` | `float` | `float` |  |  | CC: Revenues in Reporting Currency 2 |
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

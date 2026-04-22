# tfgld106

LN tfgld106 Finalized Transactions table, Generated 2026-04-10T19:41:42Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld106` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `otyp`, `odoc`, `olin`, `osrl`, `osrn` |
| **Column count** | 119 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `otyp` | `string` | `varchar` | `PK` | `not_null` | (required) Transaction Type. Max length: 3 |
| `odoc` | `integer` | `int` | `PK` | `not_null` | (required) Document |
| `olin` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `osrl` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `osrn` | `integer` | `int` | `PK` | `not_null` | (required) Background Sequence No. |
| `ocmp` | `integer` | `int` |  |  | Original Company |
| `ftyp` | `string` | `varchar` |  |  | Original Transaction Type. Max length: 3 |
| `fdoc` | `integer` | `int` |  |  | Original Document Number |
| `oyer` | `integer` | `int` |  |  | Batch Year |
| `obat` | `integer` | `int` |  |  | Batch |
| `leac` | `string` | `varchar` |  |  | Ledger Account. Max length: 12 |
| `dcdt` | `date` | `date` |  |  | Document Date |
| `typ1` | `integer` | `int` |  |  | Dimension Type 1 |
| `dim1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `typ2` | `integer` | `int` |  |  | Dimension Type 2 |
| `dim2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `typ3` | `integer` | `int` |  |  | Dimension Type 3 |
| `dim3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `typ4` | `integer` | `int` |  |  | Dimension Type 4 |
| `dim4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `typ5` | `integer` | `int` |  |  | Dimension Type 5 |
| `dim5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `refr__en_us` | `string` | `varchar` |  | `not_null` | (required) Transaction Reference - base language. Max length: 32 |
| `ref2__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 32 |
| `amnt` | `float` | `float` |  |  | Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `fact_1` | `integer` | `int` |  |  | Rate Factor |
| `fact_2` | `integer` | `int` |  |  | Rate Factor |
| `fact_3` | `integer` | `int` |  |  | Rate Factor |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `amth_1` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_2` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_3` | `float` | `float` |  |  | Amount in Home Currency |
| `dbcr` | `integer` | `int` |  |  | Debit / Credit. Allowed values: 0, 1, 2 |
| `dbcr_kw` | `string` | `varchar` |  |  | Debit / Credit (keyword). Allowed values: , tfgld.dbcr.debit, tfgld.dbcr.credit |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `vtyp` | `integer` | `int` |  |  | Tax Origin. Allowed values: 0, 1, 2, 3 |
| `vtyp_kw` | `string` | `varchar` |  |  | Tax Origin (keyword). Allowed values: , tfgld.vtyp.in, tfgld.vtyp.out, tfgld.vtyp.not.applicable |
| `vlac` | `string` | `varchar` |  |  | Tax Ledger Account. Max length: 12 |
| `vamt` | `float` | `float` |  |  | Tax Amount in Payment Currency |
| `vamh_1` | `float` | `float` |  |  | Tax Amount in Home Currency |
| `vamh_2` | `float` | `float` |  |  | Tax Amount in Home Currency |
| `vamh_3` | `float` | `float` |  |  | Tax Amount in Home Currency |
| `qty1` | `float` | `float` |  |  | Quantity 1 |
| `qty2` | `float` | `float` |  |  | Quantity 2 |
| `fprd` | `integer` | `int` |  |  | Fiscal Period |
| `fyer` | `integer` | `int` |  |  | Fiscal Year |
| `rprd` | `integer` | `int` |  |  | Reporting Period |
| `ryer` | `integer` | `int` |  |  | Reporting Year |
| `vprd` | `integer` | `int` |  |  | Tax Period |
| `vyer` | `integer` | `int` |  |  | Tax Year |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `catg` | `integer` | `int` |  |  | Transaction Category. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 |
| `catg_kw` | `string` | `varchar` |  |  | Transaction Category (keyword). Allowed values: , tfgld.catg.journals, tfgld.catg.recurr.journ, tfgld.catg.sales.inv, tfgld.catg.sales.cred, tfgld.catg.sales.corr, tfgld.catg.purchase.inv, tfgld.catg.purchase.cred, tfgld.catg.purchase.corr, tfgld.catg.opening.bal, tfgld.catg.cash, tfgld.catg.not.applicable |
| `user` | `string` | `varchar` |  |  | Last Modification By. Max length: 16 |
| `date` | `date` | `date` |  |  | Finalization Date |
| `time` | `integer` | `int` |  |  | Finalization Time |
| `trun` | `integer` | `int` |  |  | Finalization Run Number |
| `cont` | `integer` | `int` |  |  | Control Account. Allowed values: 0, 1, 2 |
| `cont_kw` | `string` | `varchar` |  |  | Control Account (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `ctyp` | `string` | `varchar` |  |  | Trans Type Correction. Max length: 3 |
| `cinv` | `integer` | `int` |  |  | Invoice Correction |
| `clin` | `integer` | `int` |  |  | Correction Line Number |
| `cdln` | `integer` | `int` |  |  | Correction Invoice Detail Line |
| `corn` | `integer` | `int` |  |  | Standing Order |
| `csrn` | `integer` | `int` |  |  | Standing Order Sequence No. |
| `tcsh` | `integer` | `int` |  |  | Type of Cash Transaction. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28 |
| `tcsh_kw` | `string` | `varchar` |  |  | Type of Cash Transaction (keyword). Allowed values: , tfcmg.tcsh.cash, tfcmg.tcsh.disc, tfcmg.tcsh.lapa, tfcmg.tcsh.pdif, tfcmg.tcsh.jrnl, tfcmg.tcsh.csts, tfcmg.tcsh.cdif, tfcmg.tcsh.ssgn, tfcmg.tcsh.cntr, tfcmg.tcsh.intr, tfcmg.tcsh.cash.ant, tfcmg.tcsh.disc.ant, tfcmg.tcsh.lapa.ant, tfcmg.tcsh.pdif.ant, tfcmg.tcsh.dedc.ant, tfcmg.tcsh.jrnl.ant, tfcmg.tcsh.cdif.ant, tfcmg.tcsh.cntr.ant, tfcmg.tcsh.intr.ant, tfcmg.tcsh.ssgn.ant, tfcmg.tcsh.facc, tfcmg.tcsh.comm.ant, tfcmg.tcsh.empty, tfcmg.tcsh.misc.exp, tfcmg.tcsh.income.tax, tfcmg.tcsh.social.contrib, tfcmg.tcsh.social.cont.exp, tfcmg.tcsh.write.off.bd |
| `tran` | `integer` | `int` |  |  | Type of Transaction. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 |
| `tran_kw` | `string` | `varchar` |  |  | Type of Transaction (keyword). Allowed values: , tfcmg.tran.customer, tfcmg.tran.supplier, tfcmg.tran.unalloc.paym, tfcmg.tran.unalloc.rec, tfcmg.tran.advance.paym, tfcmg.tran.advance.rec, tfcmg.tran.journal, tfcmg.tran.reconc.cust, tfcmg.tran.reconc.suppl |
| `dele` | `integer` | `int` |  |  | Deleted. Allowed values: 0, 1, 2 |
| `dele_kw` | `string` | `varchar` |  |  | Deleted (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `intt` | `integer` | `int` |  |  | Integration Transaction. Allowed values: 0, 1, 2 |
| `intt_kw` | `string` | `varchar` |  |  | Integration Transaction (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `adty` | `integer` | `int` |  |  | Category. Allowed values: 0, 1, 2, 3, 4, 10 |
| `adty_kw` | `string` | `varchar` |  |  | Category (keyword). Allowed values: , tfcmg.adty.tang.assets, tfcmg.adty.intang.assets, tfcmg.adty.inven, tfcmg.adty.other.assets, tfcmg.adty.not.appl |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `recl` | `integer` | `int` |  |  | Reconciliation Data Logged. Allowed values: 0, 1, 2 |
| `recl_kw` | `string` | `varchar` |  |  | Reconciliation Data Logged (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `reco` | `integer` | `int` |  |  | Reconciliation Area. Allowed values: 0, 1, 2, 5, 6, 7, 10, 11, 12, 15, 16, 20, 21, 22, 23, 28, 29, 30, 31, 33, 35, 36, 40, 42, 46, 48, 50, 52, 53, 57, 60, 99 |
| `reco_kw` | `string` | `varchar` |  |  | Reconciliation Area (keyword). Allowed values: , tcreco.inventory, tcreco.cons.inventory, tcreco.inv.accr, tcreco.cons.accr, tcreco.borr.loan.accr, tcreco.prod.order.wip, tcreco.prod.sched.wip, tcreco.pcs.wip, tcreco.ass.order.wip, tcreco.ass.line.wip, tcreco.serv.order.wip, tcreco.serv.call.wip, tcreco.maint.sales.wip, tcreco.maint.work.wip, tcreco.pur.order.wip, tcreco.pur.sched.wip, tcreco.proj.tp.wip, tcreco.proj.prov.rev, tcreco.wrk.cl.doc.wip, tcreco.trf.order.wip, tcreco.inventory.wip, tcreco.interim.cogs, tcreco.interim.rev, tcreco.interim.contr, tcreco.interim.var, tcreco.interim.trans, tcreco.interim.cust.cl, tcreco.interim.supp.cl, tcreco.commitment, tcreco.end.account, tcreco.not.applicable |
| `recs` | `integer` | `int` |  |  | Reconciliation Subarea |
| `cfrs` | `string` | `varchar` |  |  | Cash Flow Reason. Max length: 6 |
| `secd` | `string` | `varchar` |  |  | Journal Book Section. Max length: 3 |
| `tedt` | `date` | `date` |  |  | Transaction Entry Date |
| `wtsc` | `integer` | `int` |  |  | Withholding Income Tax or Social Contribution Applied. Allowed values: 1, 2 |
| `wtsc_kw` | `string` | `varchar` |  |  | Withholding Income Tax or Social Contribution Applied (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `buex` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `buex_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cacc` | `string` | `varchar` |  |  | Contra Account. Max length: 12 |
| `expd` | `integer` | `int` |  |  | Exclude from Payment Discount. Allowed values: 1, 2 |
| `expd_kw` | `string` | `varchar` |  |  | Exclude from Payment Discount (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpri` | `string` | `varchar` |  |  | BP Identification Number. Max length: 35 |
| `regc` | `string` | `varchar` |  |  | Registration Code. Max length: 9 |
| `txta` | `integer` | `int` |  |  | Transaction Text |
| `regc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax221 Jurisdictions by Registration |
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

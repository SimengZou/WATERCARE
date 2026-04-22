# tfacp270

LN tfacp270 Purchase Order Related Financial Transactions table, Generated 2026-04-10T19:41:30Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp270` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `guid`, `dbcr` |
| **Column count** | 42 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `guid` | `string` | `varchar` | `PK` | `not_null` | (required) Unique Identifier. Max length: 22 |
| `dbcr` | `integer` | `int` | `PK` | `not_null` | (required) Debit/Credit. Allowed values: 1, 2 |
| `dbcr_kw` | `string` | `varchar` |  |  | Debit/Credit (keyword). Allowed values: tfgld.dbcr.debit, tfgld.dbcr.credit |
| `idtc` | `string` | `varchar` |  |  | Integration Document Type. Max length: 8 |
| `ktrn` | `integer` | `int` |  |  | Kind of Entry. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 |
| `ktrn_kw` | `string` | `varchar` |  |  | Kind of Entry (keyword). Allowed values: tcktrn.integ.trans, tcktrn.approval, tcktrn.unapproval, tcktrn.corr.non.final, tcktrn.cor.r.non.final, tcktrn.corr.final, tcktrn.cost.trans, tcktrn.exp.tax, tcktrn.open.balance, tcktrn.curr.diff, tcktrn.rounding.diff, tcktrn.sales.invoice, tcktrn.reversal.int.tr, tcktrn.reversed.int.tr, tcktrn.advance.cmg, tcktrn.gain.loss, tcktrn.tax.corr, tcktrn.rec.asset.exp, tcktrn.rec.asset.cap, tcktrn.rec.asset.cor |
| `reco` | `integer` | `int` |  |  | Reconciliation Area. Allowed values: 1, 2, 5, 6, 7, 10, 11, 12, 15, 16, 20, 21, 22, 23, 28, 29, 30, 31, 33, 35, 36, 40, 42, 46, 48, 50, 52, 53, 57, 60, 99 |
| `reco_kw` | `string` | `varchar` |  |  | Reconciliation Area (keyword). Allowed values: tcreco.inventory, tcreco.cons.inventory, tcreco.inv.accr, tcreco.cons.accr, tcreco.borr.loan.accr, tcreco.prod.order.wip, tcreco.prod.sched.wip, tcreco.pcs.wip, tcreco.ass.order.wip, tcreco.ass.line.wip, tcreco.serv.order.wip, tcreco.serv.call.wip, tcreco.maint.sales.wip, tcreco.maint.work.wip, tcreco.pur.order.wip, tcreco.pur.sched.wip, tcreco.proj.tp.wip, tcreco.proj.prov.rev, tcreco.wrk.cl.doc.wip, tcreco.trf.order.wip, tcreco.inventory.wip, tcreco.interim.cogs, tcreco.interim.rev, tcreco.interim.contr, tcreco.interim.var, tcreco.interim.trans, tcreco.interim.cust.cl, tcreco.interim.supp.cl, tcreco.commitment, tcreco.end.account, tcreco.not.applicable |
| `recs` | `integer` | `int` |  |  | Reconciliation Subarea |
| `rbon` | `string` | `varchar` |  |  | Business Object. Max length: 17 |
| `rbid` | `string` | `varchar` |  |  | Business Object ID. Max length: 11 |
| `rpon` | `integer` | `int` |  |  | Sort Position |
| `obre` | `string` | `varchar` |  |  | Business Object Reference. Max length: 90 |
| `buid` | `string` | `varchar` |  |  | Business Object Reference GUID. Max length: 22 |
| `txin` | `integer` | `int` |  |  | Tax Indicator. Allowed values: 10, 255 |
| `txin_kw` | `string` | `varchar` |  |  | Tax Indicator (keyword). Allowed values: tctax.indi.expense.tax, tctax.indi.not.appl |
| `ocmp` | `integer` | `int` |  |  | Logistic Company |
| `fcom` | `integer` | `int` |  |  | Finance Company |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `nuni` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `amnt` | `float` | `float` |  |  | Transaction Amount |
| `ccur` | `string` | `varchar` |  |  | Transaction Currency. Max length: 3 |
| `amth_1` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_2` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_3` | `float` | `float` |  |  | Amount in Home Currency |
| `dcom` | `integer` | `int` |  |  | Document Company |
| `ttyp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `docn` | `integer` | `int` |  |  | Document |
| `clss` | `integer` | `int` |  |  | Closing Sequence |
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

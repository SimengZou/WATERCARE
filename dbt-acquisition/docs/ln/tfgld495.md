# tfgld495

LN tfgld495 Reconciliation Data table, Generated 2026-04-10T19:41:45Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld495` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `guid`, `dbcr` |
| **Column count** | 93 |

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
| `ocmp` | `integer` | `int` |  |  | Logistic Company |
| `trdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction date (UTC) |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `amnt` | `float` | `float` |  |  | Transaction Amount |
| `ccur` | `string` | `varchar` |  |  | Transaction Currency. Max length: 3 |
| `amth_1` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_2` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_3` | `float` | `float` |  |  | Amount in Home Currency |
| `nuni` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `leac` | `string` | `varchar` |  |  | Ledger Account. Max length: 12 |
| `dim1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `dim2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `dim3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `dim4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `dim5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `fyer` | `integer` | `int` |  |  | Fiscal Year |
| `fprd` | `integer` | `int` |  |  | Fiscal Period |
| `fcom` | `integer` | `int` |  |  | Finance Company |
| `year` | `integer` | `int` |  |  | Batch Year |
| `btno` | `integer` | `int` |  |  | Batch |
| `ttyp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `docn` | `integer` | `int` |  |  | Document |
| `lino` | `integer` | `int` |  |  | Line |
| `serl` | `integer` | `int` |  |  | Sequence Number |
| `srno` | `integer` | `int` |  |  | Background Sequence No. |
| `dcdt` | `date` | `date` |  |  | Document Date |
| `podt` | `timestamp` | `timestamp_ntz` |  |  | Posting Date |
| `rev1` | `string` | `varchar` |  |  | Reconciliation Value (Element 1). Max length: 50 |
| `rev2` | `string` | `varchar` |  |  | Reconciliation Value (Element 2). Max length: 50 |
| `rev3` | `string` | `varchar` |  |  | Reconciliation Value (Element 3). Max length: 50 |
| `rev4` | `string` | `varchar` |  |  | Reconciliation Value (Element 4). Max length: 50 |
| `rev5` | `string` | `varchar` |  |  | Reconciliation Value (Element 5). Max length: 50 |
| `urat_1` | `float` | `float` |  |  | Used Currency Rate |
| `urat_2` | `float` | `float` |  |  | Used Currency Rate |
| `urat_3` | `float` | `float` |  |  | Used Currency Rate |
| `arat_1` | `float` | `float` |  |  | Actual Currency Rate |
| `arat_2` | `float` | `float` |  |  | Actual Currency Rate |
| `arat_3` | `float` | `float` |  |  | Actual Currency Rate |
| `inic` | `integer` | `int` |  |  | Intercompany Indicator. Allowed values: 1, 2 |
| `inic_kw` | `string` | `varchar` |  |  | Intercompany Indicator (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inad` | `integer` | `int` |  |  | Antedating Indicator. Allowed values: 1, 2 |
| `inad_kw` | `string` | `varchar` |  |  | Antedating Indicator (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acce` | `integer` | `int` |  |  | Accepted. Allowed values: 1, 2 |
| `acce_kw` | `string` | `varchar` |  |  | Accepted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `accf` | `integer` | `int` |  |  | Final Accepted. Allowed values: 1, 2 |
| `accf_kw` | `string` | `varchar` |  |  | Final Accepted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rce1` | `string` | `varchar` |  |  | Reconciliation (Element 1). Max length: 12 |
| `rce2` | `string` | `varchar` |  |  | Reconciliation (Element 2). Max length: 12 |
| `rce3` | `string` | `varchar` |  |  | Reconciliation (Element 3). Max length: 12 |
| `rce4` | `string` | `varchar` |  |  | Reconciliation (Element 4). Max length: 12 |
| `rce5` | `string` | `varchar` |  |  | Reconciliation (Element 5). Max length: 12 |
| `tdte` | `date` | `date` |  |  | Transaction Date (Local) |
| `link` | `string` | `varchar` |  |  | Reference Link. Max length: 22 |
| `proj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `lcit` | `integer` | `int` |  |  | Logical Company of Integration Transaction |
| `ogui` | `string` | `varchar` |  |  | Original GUID. Max length: 22 |
| `odbc` | `integer` | `int` |  |  | Original Debit/Credit. Allowed values: 1, 2 |
| `odbc_kw` | `string` | `varchar` |  |  | Original Debit/Credit (keyword). Allowed values: tfgld.dbcr.debit, tfgld.dbcr.credit |
| `blrf` | `string` | `varchar` |  |  | Balance Reference. Max length: 90 |
| `aobl` | `integer` | `int` |  |  | Automatic Opening Balance. Allowed values: 1, 2 |
| `aobl_kw` | `string` | `varchar` |  |  | Automatic Opening Balance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
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

# whina126

LN whina126 Inventory Receipt Transaction Variances table, Generated 2026-04-10T19:42:46Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina126` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `cwar`, `trdt`, `seqn`, `inwp`, `vpdt`, `vseq` |
| **Column count** | 31 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `trdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Transaction Date |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `inwp` | `integer` | `int` | `PK` | `not_null` | (required) Inventory WIP. Allowed values: 1, 2 |
| `inwp_kw` | `string` | `varchar` |  |  | Inventory WIP (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vpdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Inventory Variance Process Date |
| `vseq` | `integer` | `int` | `PK` | `not_null` | (required) Variance Sequence |
| `lgdt` | `timestamp` | `timestamp_ntz` |  |  | Actual Log Date |
| `reva` | `integer` | `int` |  |  | Revaluation. Allowed values: 1, 2 |
| `reva_kw` | `string` | `varchar` |  |  | Revaluation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iror` | `integer` | `int` |  |  | Inventory Variance Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 100 |
| `iror_kw` | `string` | `varchar` |  |  | Inventory Variance Origin (keyword). Allowed values: tcvar.orig.price.change, tcvar.orig.invoice.var, tcvar.orig.prod.close, tcvar.orig.expensed.tax, tcvar.orig.efficiency.var, tcvar.orig.prod.price.var, tcvar.orig.lc.price.change, tcvar.orig.lc.invoice.var, tcvar.orig.lc.expensed.tax, tcvar.orig.curr.var.st.pm, tcvar.orig.exp.tax.st.pm, tcvar.orig.curr.var, tcvar.orig.curr.gain.loss, tcvar.orig.curr.var.lc, tcvar.orig.tax.corr, tcvar.orig.lc.tax.corr, tcvar.orig.exp.tax.reten, tcvar.orig.exp.tax.ret.rel, tcvar.orig.measure.result, tcvar.orig.prod.result, tcvar.orig.value.correc, tcvar.orig.not.appl |
| `ivsq` | `integer` | `int` |  |  | Inventory Variance Sequence |
| `rorg` | `integer` | `int` |  |  | Revaluation Origin. Allowed values: 10, 20, 30, 35, 40, 50, 100 |
| `rorg_kw` | `string` | `varchar` |  |  | Revaluation Origin (keyword). Allowed values: whina.rorg.act.cost.price, whina.rorg.antedating, whina.rorg.mauc.correct, whina.rorg.act.cost.corr, whina.rorg.change.val.meth, whina.rorg.change.int.fam, whina.rorg.not.appl |
| `rorn` | `string` | `varchar` |  |  | Revaluation Order. Max length: 9 |
| `rseq` | `integer` | `int` |  |  | Revaluation Order Sequence |
| `item_cwar_trdt_seqn_inwp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina112 Inventory Receipt Transactions |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd400 Item Warehousing Data |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd200 Warehouses |
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

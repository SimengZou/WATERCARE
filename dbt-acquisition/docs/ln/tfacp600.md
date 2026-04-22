# tfacp600

LN tfacp600 Open Items Payment-related Documents A/P table, Generated 2026-04-10T19:41:31Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp600` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pcom`, `payt`, `payd`, `payl`, `seqn` |
| **Column count** | 44 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pcom` | `integer` | `int` | `PK` | `not_null` | (required) Original Payment Company |
| `payt` | `string` | `varchar` | `PK` | `not_null` | (required) Original Payment Transaction Type. Max length: 3 |
| `payd` | `integer` | `int` | `PK` | `not_null` | (required) Original Payment Document Number |
| `payl` | `integer` | `int` | `PK` | `not_null` | (required) Original Payment Line Number |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `pyid` | `string` | `varchar` |  |  | Original Payment ID. Max length: 15 |
| `ipco` | `integer` | `int` |  |  | Initial Payment Company |
| `ipty` | `string` | `varchar` |  |  | Initial Payment Transaction Type. Max length: 3 |
| `ipdo` | `integer` | `int` |  |  | Initial Payment Document Number |
| `ipli` | `integer` | `int` |  |  | Initial Payment Line Number |
| `tpay` | `integer` | `int` |  |  | Type of Transaction. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 |
| `tpay_kw` | `string` | `varchar` |  |  | Type of Transaction (keyword). Allowed values: tfacp.tpay.invoice, tfacp.tpay.normal, tfacp.tpay.sales, tfacp.tpay.credit, tfacp.tpay.correction, tfacp.tpay.currency, tfacp.tpay.payment, tfacp.tpay.anticipated, tfacp.tpay.advance, tfacp.tpay.unallocated, tfacp.tpay.advance.ant, tfacp.tpay.unallocated.ant, tfacp.tpay.standing, tfacp.tpay.assignment, tfacp.tpay.trade.notes, tfacp.tpay.cash.journal |
| `pbcp` | `integer` | `int` |  |  | Payment Batch Company |
| `pbtn` | `integer` | `int` |  |  | Original Payment Batch |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `bank` | `string` | `varchar` |  |  | Bank Relation. Max length: 3 |
| `basu` | `string` | `varchar` |  |  | Supplier's Bank. Max length: 3 |
| `ptbp` | `string` | `varchar` |  |  | Pay-to Business Partner. Max length: 9 |
| `tpbp` | `string` | `varchar` |  |  | Top Parent Business Partner. Max length: 9 |
| `ddat` | `date` | `date` |  |  | Document Date |
| `amnt` | `float` | `float` |  |  | Amount in Document Currency |
| `curr` | `string` | `varchar` |  |  | Transaction Currency. Max length: 3 |
| `amtl` | `float` | `float` |  |  | Amount in Local Currency |
| `step` | `integer` | `int` |  |  | Payment Procedure Step. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 |
| `step_kw` | `string` | `varchar` |  |  | Payment Procedure Step (keyword). Allowed values: tfcmg.step.doc.selected, tfcmg.step.doc.open, tfcmg.step.doc.received, tfcmg.step.doc.issued, tfcmg.step.doc.to.cust, tfcmg.step.doc.acc.by.cust, tfcmg.step.doc.acc.sent, tfcmg.step.doc.collateral, tfcmg.step.doc.sent.disc, tfcmg.step.doc.to.bank, tfcmg.step.doc.endorsed, tfcmg.step.doc.discounted, tfcmg.step.doc.cancelled, tfcmg.step.doc.matured, tfcmg.step.doc.void, tfcmg.step.doc.paid, tfcmg.step.doc.rejected, tfcmg.step.doc.dishonored, tfcmg.step.doc.settled, tfcmg.step.empty |
| `gcom` | `integer` | `int` |  |  | Payment Company |
| `gtyp` | `string` | `varchar` |  |  | Payment Transaction Type. Max length: 3 |
| `gdoc` | `integer` | `int` |  |  | Payment Document Number |
| `glin` | `integer` | `int` |  |  | Payment Line Number |
| `reas` | `string` | `varchar` |  |  | Reason for Payment. Max length: 3 |
| `usrc` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `tpbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
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

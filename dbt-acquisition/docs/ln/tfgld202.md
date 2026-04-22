# tfgld202

LN tfgld202 History - Dimension/Ledger Account Totals table, Generated 2026-04-10T19:41:43Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld202` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `ptyp`, `year`, `prno`, `dtyp`, `dimx`, `leac`, `ccur`, `duac`, `bpid` |
| **Column count** | 43 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `ptyp` | `integer` | `int` | `PK` | `not_null` | (required) Period Type. Allowed values: 0, 1, 2, 3 |
| `ptyp_kw` | `string` | `varchar` |  |  | Period Type (keyword). Allowed values: , tfgld.ptyp.financial, tfgld.ptyp.reporting, tfgld.ptyp.vat |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Fiscal Year |
| `prno` | `integer` | `int` | `PK` | `not_null` | (required) Period |
| `dtyp` | `integer` | `int` | `PK` | `not_null` | (required) Dimension Type |
| `dimx` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension. Max length: 9 |
| `leac` | `string` | `varchar` | `PK` | `not_null` | (required) Ledger Account. Max length: 12 |
| `ccur` | `string` | `varchar` | `PK` | `not_null` | (required) Currency. Max length: 3 |
| `duac` | `integer` | `int` | `PK` | `not_null` | (required) Dual Accounting Indicator. Allowed values: 0, 1, 2 |
| `duac_kw` | `string` | `varchar` |  |  | Dual Accounting Indicator (keyword). Allowed values: , tfgld.duac.statutory, tfgld.duac.complem |
| `bpid` | `string` | `varchar` | `PK` | `not_null` | (required) Business Partner. Max length: 9 |
| `fdam` | `float` | `float` |  |  | Finalized Debit Amount |
| `fcam` | `float` | `float` |  |  | Finalized Credit Amount |
| `fdah_1` | `float` | `float` |  |  | Finalized Debit Amount in HC |
| `fdah_2` | `float` | `float` |  |  | Finalized Debit Amount in HC |
| `fdah_3` | `float` | `float` |  |  | Finalized Debit Amount in HC |
| `fcah_1` | `float` | `float` |  |  | Finalized Credit Amount in HC |
| `fcah_2` | `float` | `float` |  |  | Finalized Credit Amount in HC |
| `fcah_3` | `float` | `float` |  |  | Finalized Credit Amount in HC |
| `fqt1` | `float` | `float` |  |  | Finalized Quantity 1 |
| `fqt2` | `float` | `float` |  |  | Finalized Quantity 2 |
| `ndam` | `float` | `float` |  |  | Not Final Debit Amount |
| `ncam` | `float` | `float` |  |  | Not Final Credit Amount |
| `ndah_1` | `float` | `float` |  |  | Not Final Debit Amount in HC |
| `ndah_2` | `float` | `float` |  |  | Not Final Debit Amount in HC |
| `ndah_3` | `float` | `float` |  |  | Not Final Debit Amount in HC |
| `ncah_1` | `float` | `float` |  |  | Not Final Credit Amount in HC |
| `ncah_2` | `float` | `float` |  |  | Not Final Credit Amount in HC |
| `ncah_3` | `float` | `float` |  |  | Not Final Credit Amount in HC |
| `nqt1` | `float` | `float` |  |  | Not Final Quantity 1 |
| `nqt2` | `float` | `float` |  |  | Not Final Quantity 2 |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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

# tfgld100

LN tfgld100 Financial Batch table, Generated 2026-04-10T19:41:42Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `year`, `btno` |
| **Column count** | 33 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Fiscal Year |
| `btno` | `integer` | `int` | `PK` | `not_null` | (required) Batch |
| `stat` | `integer` | `int` |  |  | Batch Status. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8 |
| `stat_kw` | `string` | `varchar` |  |  | Batch Status (keyword). Allowed values: , tfgld.bstt.open.free, tfgld.bstt.in.use, tfgld.bstt.in.back, tfgld.bstt.ready, tfgld.bstt.in.term, tfgld.bstt.term, tfgld.bstt.deleted, tfgld.bstt.errors |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `tedt` | `date` | `date` |  |  | Transaction Entry Date |
| `trdt` | `date` | `date` |  |  | Finalization Date |
| `itbc` | `integer` | `int` |  |  | Batch Modifiable. Allowed values: 0, 1, 2 |
| `itbc_kw` | `string` | `varchar` |  |  | Batch Modifiable (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `fprd` | `integer` | `int` |  |  | Fiscal Period |
| `rprd` | `integer` | `int` |  |  | Reporting Period |
| `vprd` | `integer` | `int` |  |  | Tax Period |
| `vyer` | `integer` | `int` |  |  | Tax Year |
| `bref__en_us` | `string` | `varchar` |  | `not_null` | (required) Batch Reference - base language. Max length: 30 |
| `btyp` | `integer` | `int` |  |  | Access Mode for Batch. Allowed values: 0, 1, 2 |
| `btyp_kw` | `string` | `varchar` |  |  | Access Mode for Batch (keyword). Allowed values: , tfgld.btyp.multi.user, tfgld.btyp.single.user |
| `trun` | `integer` | `int` |  |  | Finalization Run Number |
| `tstt` | `integer` | `int` |  |  | Finalization Status. Allowed values: 0, 1, 2, 3, 4 |
| `tstt_kw` | `string` | `varchar` |  |  | Finalization Status (keyword). Allowed values: , tfgld.tstt.free, tfgld.tstt.selected, tfgld.tstt.errors, tfgld.tstt.terminated |
| `selt` | `integer` | `int` |  |  | Select. Allowed values: 0, 1, 2 |
| `selt_kw` | `string` | `varchar` |  |  | Select (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `year_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld006 End Dates by Year |
| `vyer_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld006 End Dates by Year |
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

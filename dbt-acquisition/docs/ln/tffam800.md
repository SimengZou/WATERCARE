# tffam800

LN tffam800 Transaction table, Generated 2026-04-10T19:41:34Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam800` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `tkey` |
| **Column count** | 34 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `tkey` | `integer` | `int` | `PK` | `not_null` | (required) Transaction Key |
| `anbr` | `string` | `varchar` |  |  | Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` |  |  | Asset Extension. Max length: 15 |
| `amnt_1` | `float` | `float` |  |  | Transaction Amount |
| `amnt_2` | `float` | `float` |  |  | Transaction Amount |
| `amnt_3` | `float` | `float` |  |  | Transaction Amount |
| `atty` | `integer` | `int` |  |  | Asset Tran Type. Allowed values: 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 45, 46, 50 |
| `atty_kw` | `string` | `varchar` |  |  | Asset Tran Type (keyword). Allowed values: tffam.atty.acqu, tffam.atty.adju, tffam.atty.depr, tffam.atty.disp, tffam.atty.tran.in, tffam.atty.tran.out, tffam.atty.add.post, tffam.atty.remo.acqu, tffam.atty.remo.capi, tffam.atty.reva, tffam.atty.rest.depr, tffam.atty.adju.depr, tffam.atty.tran.depr, tffam.atty.addi.post, tffam.atty.gain.loss, tffam.atty.susp.depr, tffam.atty.rest.susp.depr, tffam.atty.anti.depr, tffam.atty.rest.anti.depr, tffam.atty.reva.depr, tffam.atty.rest.reva.depr, tffam.atty.tran.reva.depr, tffam.atty.tran.reva.in, tffam.atty.tran.reva.out, tffam.atty.anti.reva.depr, tffam.atty.rest.anti.rdpr, tffam.atty.econ.reva.depr, tffam.atty.retr.reva.depr, tffam.atty.accu.reva, tffam.atty.reva.cz, tffam.atty.tu, tffam.atty.expense |
| `book` | `string` | `varchar` |  |  | Asset Book. Max length: 10 |
| `docn` | `integer` | `int` |  |  | Document Number |
| `line` | `integer` | `int` |  |  | Line Number |
| `rort` | `integer` | `int` |  |  | Reversed Transaction. Allowed values: 1, 2 |
| `rort_kw` | `string` | `varchar` |  |  | Reversed Transaction (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prod` | `integer` | `int` |  |  | Period |
| `post` | `integer` | `int` |  |  | Posted. Allowed values: 1, 2 |
| `post_kw` | `string` | `varchar` |  |  | Posted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tdat` | `date` | `date` |  |  | Transaction Date |
| `ttyp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `year` | `integer` | `int` |  |  | Year |
| `text` | `integer` | `int` |  |  | Journal Entry Text |
| `anbr_aext_book_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam110 Asset Book |
| `anbr_aext_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam100 Asset |
| `book_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam600 Books |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

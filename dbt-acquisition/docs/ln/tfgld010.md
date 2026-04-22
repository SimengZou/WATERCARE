# tfgld010

LN tfgld010 Dimensions table, Generated 2026-04-10T19:41:41Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld010` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `dtyp`, `dimx` |
| **Column count** | 37 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `dtyp` | `integer` | `int` | `PK` | `not_null` | (required) Dimension Type |
| `dimx` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension. Max length: 9 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Dimension Description - base language. Max length: 30 |
| `subl` | `integer` | `int` |  |  | Dimension Sublevel |
| `pdix` | `string` | `varchar` |  |  | Parent Dimension. Max length: 9 |
| `uni1` | `integer` | `int` |  |  | Display Unit 1. Allowed values: 1, 2 |
| `uni1_kw` | `string` | `varchar` |  |  | Display Unit 1 (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uni2` | `integer` | `int` |  |  | Display Unit 2. Allowed values: 1, 2 |
| `uni2_kw` | `string` | `varchar` |  |  | Display Unit 2 (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `emno` | `string` | `varchar` |  |  | Person Responsible. Max length: 9 |
| `pseq` | `integer` | `int` |  |  | Print Sequence |
| `bloc` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3 |
| `bloc_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tfgld.bloc.free, tfgld.bloc.manual.input, tfgld.bloc.all.input |
| `bfdt` | `date` | `date` |  |  | Blocking Effective From |
| `budt` | `date` | `date` |  |  | Blocking Effective To |
| `atyp` | `integer` | `int` |  |  | Dimension Type. Allowed values: 1, 2 |
| `atyp_kw` | `string` | `varchar` |  |  | Dimension Type (keyword). Allowed values: tfgld.datp.normal, tfgld.datp.text |
| `skey__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `qan1` | `string` | `varchar` |  |  | Unit 1. Max length: 3 |
| `qan2` | `string` | `varchar` |  |  | Unit 2. Max length: 3 |
| `text` | `integer` | `int` |  |  | Text |
| `dtyp_pdix_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `qan1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `qan2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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

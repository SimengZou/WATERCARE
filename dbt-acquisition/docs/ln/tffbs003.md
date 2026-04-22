# tffbs003

LN tffbs003 Budget table, Generated 2026-04-10T19:41:37Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffbs003` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `budg` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `budg` | `string` | `varchar` | `PK` | `not_null` | (required) Budget. Max length: 3 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `sdbu` | `integer` | `int` |  |  | Flexible Budget. Allowed values: 1, 2 |
| `sdbu_kw` | `string` | `varchar` |  |  | Flexible Budget (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `budm` | `integer` | `int` |  |  | Budgeting Method. Allowed values: 1, 2 |
| `budm_kw` | `string` | `varchar` |  |  | Budgeting Method (keyword). Allowed values: tffbs.budm.bottom.up, tffbs.budm.top.down |
| `llac` | `integer` | `int` |  |  | Link with Ledger Account. Allowed values: 0, 1, 2 |
| `llac_kw` | `string` | `varchar` |  |  | Link with Ledger Account (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `adt1` | `integer` | `int` |  |  | Dimension Type 1 Applicable. Allowed values: 1, 2 |
| `adt1_kw` | `string` | `varchar` |  |  | Dimension Type 1 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt2` | `integer` | `int` |  |  | Dimension Type 2 Applicable. Allowed values: 1, 2 |
| `adt2_kw` | `string` | `varchar` |  |  | Dimension Type 2 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt3` | `integer` | `int` |  |  | Dimension Type 3 Applicable. Allowed values: 1, 2 |
| `adt3_kw` | `string` | `varchar` |  |  | Dimension Type 3 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt4` | `integer` | `int` |  |  | Dimension Type 4 Applicable. Allowed values: 1, 2 |
| `adt4_kw` | `string` | `varchar` |  |  | Dimension Type 4 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt5` | `integer` | `int` |  |  | Dimension Type 5 Applicable. Allowed values: 1, 2 |
| `adt5_kw` | `string` | `varchar` |  |  | Dimension Type 5 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt6` | `integer` | `int` |  |  | Dimension Type 6 Applicable. Allowed values: 1, 2 |
| `adt6_kw` | `string` | `varchar` |  |  | Dimension Type 6 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt7` | `integer` | `int` |  |  | Dimension Type 7 Applicable. Allowed values: 1, 2 |
| `adt7_kw` | `string` | `varchar` |  |  | Dimension Type 7 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt8` | `integer` | `int` |  |  | Dimension Type 8 Applicable. Allowed values: 1, 2 |
| `adt8_kw` | `string` | `varchar` |  |  | Dimension Type 8 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adt9` | `integer` | `int` |  |  | Dimension Type 9 Applicable. Allowed values: 1, 2 |
| `adt9_kw` | `string` | `varchar` |  |  | Dimension Type 9 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ad10` | `integer` | `int` |  |  | Dimension Type 10 Applicable. Allowed values: 1, 2 |
| `ad10_kw` | `string` | `varchar` |  |  | Dimension Type 10 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ad11` | `integer` | `int` |  |  | Dimension Type 11 Applicable. Allowed values: 1, 2 |
| `ad11_kw` | `string` | `varchar` |  |  | Dimension Type 11 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ad12` | `integer` | `int` |  |  | Dimension Type 12 Applicable. Allowed values: 1, 2 |
| `ad12_kw` | `string` | `varchar` |  |  | Dimension Type 12 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aqu1` | `integer` | `int` |  |  | Quantity 1 Applicable. Allowed values: 1, 2 |
| `aqu1_kw` | `string` | `varchar` |  |  | Quantity 1 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aqu2` | `integer` | `int` |  |  | Quantity 2 Applicable. Allowed values: 1, 2 |
| `aqu2_kw` | `string` | `varchar` |  |  | Quantity 2 Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nbpr` | `integer` | `int` |  |  | Number of Budget Periods |
| `text` | `integer` | `int` |  |  | Text |
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

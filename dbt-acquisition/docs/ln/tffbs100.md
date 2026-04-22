# tffbs100

LN tffbs100 Budget Amounts and Quantities per Year table, Generated 2026-04-10T19:41:37Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffbs100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `year`, `budg`, `leac`, `dim1`, `dim2`, `dim3`, `dim4`, `dim5`, `dim6`, `dim7`, `dim8`, `dim9`, `dm10`, `dm11`, `dm12` |
| **Column count** | 76 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Year |
| `budg` | `string` | `varchar` | `PK` | `not_null` | (required) Budget. Max length: 3 |
| `leac` | `string` | `varchar` | `PK` | `not_null` | (required) Ledger Account. Max length: 12 |
| `dim1` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 1. Max length: 9 |
| `dim2` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 2. Max length: 9 |
| `dim3` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 3. Max length: 9 |
| `dim4` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 4. Max length: 9 |
| `dim5` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 12. Max length: 9 |
| `dbcr` | `integer` | `int` |  |  | Debit/Credit Indicator. Allowed values: 1, 2 |
| `dbcr_kw` | `string` | `varchar` |  |  | Debit/Credit Indicator (keyword). Allowed values: tfgld.dbcr.debit, tfgld.dbcr.credit |
| `amnt` | `float` | `float` |  |  | Amount |
| `qan1` | `float` | `float` |  |  | Quantity 1 |
| `qan2` | `float` | `float` |  |  | Quantity 2 |
| `pdmo` | `integer` | `int` |  |  | Periodic Distribution Mode. Allowed values: 1, 2, 3 |
| `pdmo_kw` | `string` | `varchar` |  |  | Periodic Distribution Mode (keyword). Allowed values: tffbs.pdmo.distribution, tffbs.pdmo.equall, tffbs.pdmo.not.app |
| `disb` | `string` | `varchar` |  |  | Distribution. Max length: 3 |
| `dty1` | `integer` | `int` |  |  | Dimension Type 1 |
| `dty2` | `integer` | `int` |  |  | Dimension Type 2 |
| `dty3` | `integer` | `int` |  |  | Dimension Type 3 |
| `dty4` | `integer` | `int` |  |  | Dimension Type 4 |
| `dty5` | `integer` | `int` |  |  | Dimension Type 5 |
| `dty6` | `integer` | `int` |  |  | Dimension Type 6 |
| `dty7` | `integer` | `int` |  |  | Dimension Type 7 |
| `dty8` | `integer` | `int` |  |  | Dimension Type 8 |
| `dty9` | `integer` | `int` |  |  | Dimension Type 9 |
| `dt10` | `integer` | `int` |  |  | Dimension Type 10 |
| `dt11` | `integer` | `int` |  |  | Dimension Type 11 |
| `dt12` | `integer` | `int` |  |  | Dimension Type 12 |
| `back` | `integer` | `int` |  |  | Created in Background. Allowed values: 1, 2 |
| `back_kw` | `string` | `varchar` |  |  | Created in Background (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `olap` | `integer` | `int` |  |  | Send to OLAP |
| `year_budg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs005 Budgets per Year |
| `budg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs003 Budget |
| `leac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `disb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffbs001 Budget Distribution |
| `dty1_dim1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty2_dim2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty3_dim3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty4_dim4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty5_dim5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty6_dim6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty7_dim7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty8_dim8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dty9_dim9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dty9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dt10_dm10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dt10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dt11_dm11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dt11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
| `dt12_dm12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dt12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld002 Dimension Descriptions |
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

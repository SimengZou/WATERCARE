# tfgld003

LN tfgld003 Group Company Parameters table, Generated 2026-04-10T19:41:39Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld003` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt` |
| **Column count** | 61 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Effective Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `dhcu__en_us` | `string` | `varchar` |  | `not_null` | (required) Short Description of Currency - base language. Max length: 9 |
| `dim1` | `integer` | `int` |  |  | Dimension 1 Used Y/N. Allowed values: 1, 2 |
| `dim1_kw` | `string` | `varchar` |  |  | Dimension 1 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim2` | `integer` | `int` |  |  | Dimension 2 Used Y/N. Allowed values: 1, 2 |
| `dim2_kw` | `string` | `varchar` |  |  | Dimension 2 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim3` | `integer` | `int` |  |  | Dimension 3 Used Y/N. Allowed values: 1, 2 |
| `dim3_kw` | `string` | `varchar` |  |  | Dimension 3 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim4` | `integer` | `int` |  |  | Dimension 4 Used Y/N. Allowed values: 1, 2 |
| `dim4_kw` | `string` | `varchar` |  |  | Dimension 4 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim5` | `integer` | `int` |  |  | Dimension 5 Used Y/N. Allowed values: 1, 2 |
| `dim5_kw` | `string` | `varchar` |  |  | Dimension 5 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim6` | `integer` | `int` |  |  | Dimension 6 Used Y/N. Allowed values: 1, 2 |
| `dim6_kw` | `string` | `varchar` |  |  | Dimension 6 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim7` | `integer` | `int` |  |  | Dimension 7 Used Y/N. Allowed values: 1, 2 |
| `dim7_kw` | `string` | `varchar` |  |  | Dimension 7 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim8` | `integer` | `int` |  |  | Dimension 8 Used Y/N. Allowed values: 1, 2 |
| `dim8_kw` | `string` | `varchar` |  |  | Dimension 8 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dim9` | `integer` | `int` |  |  | Dimension 9 Used Y/N. Allowed values: 1, 2 |
| `dim9_kw` | `string` | `varchar` |  |  | Dimension 9 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dm10` | `integer` | `int` |  |  | Dimension 10 Used Y/N. Allowed values: 1, 2 |
| `dm10_kw` | `string` | `varchar` |  |  | Dimension 10 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dm11` | `integer` | `int` |  |  | Dimension 11 Used Y/N. Allowed values: 1, 2 |
| `dm11_kw` | `string` | `varchar` |  |  | Dimension 11 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dm12` | `integer` | `int` |  |  | Dimension 12 Used Y/N. Allowed values: 1, 2 |
| `dm12_kw` | `string` | `varchar` |  |  | Dimension 12 Used Y/N (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rper` | `integer` | `int` |  |  | Reporting Periods. Allowed values: 1, 2 |
| `rper_kw` | `string` | `varchar` |  |  | Reporting Periods (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `nfpr` | `integer` | `int` |  |  | Number of Fiscal Periods |
| `nrpr` | `integer` | `int` |  |  | Number of Reporting Periods |
| `nvpr` | `integer` | `int` |  |  | Number of Tax Periods |
| `sdat` | `integer` | `int` |  |  | Store Data for X Years |
| `bcmp` | `integer` | `int` |  |  | Base Company |
| `psep` | `string` | `varchar` |  |  | Period Separator. Max length: 1 |
| `cfst` | `integer` | `int` |  |  | Cash Flow Statement. Allowed values: 1, 2 |
| `cfst_kw` | `string` | `varchar` |  |  | Cash Flow Statement (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dcfi` | `integer` | `int` |  |  | Detailed Cash Flow Information. Allowed values: 1, 2 |
| `dcfi_kw` | `string` | `varchar` |  |  | Detailed Cash Flow Information (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `papp` | `integer` | `int` |  |  | Payment Agreement. Allowed values: 1, 2 |
| `papp_kw` | `string` | `varchar` |  |  | Payment Agreement (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `gmbi` | `integer` | `int` |  |  | Monthly Billing Invoices. Allowed values: 1, 2 |
| `gmbi_kw` | `string` | `varchar` |  |  | Monthly Billing Invoices (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sgmr` | `integer` | `int` |  |  | Segment Reporting. Allowed values: 1, 2 |
| `sgmr_kw` | `string` | `varchar` |  |  | Segment Reporting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psic` | `integer` | `int` |  |  | Prevent Intercompany Transactions by Segment. Allowed values: 1, 2 |
| `psic_kw` | `string` | `varchar` |  |  | Prevent Intercompany Transactions by Segment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psbk` | `integer` | `int` |  |  | Prevent Bank Transactions by Segment. Allowed values: 1, 2 |
| `psbk_kw` | `string` | `varchar` |  |  | Prevent Bank Transactions by Segment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pupt` | `integer` | `int` |  |  | Postpone Updating Period Totals - Cash Flow. Allowed values: 1, 2 |
| `pupt_kw` | `string` | `varchar` |  |  | Postpone Updating Period Totals - Cash Flow (keyword). Allowed values: tcyesno.yes, tcyesno.no |
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

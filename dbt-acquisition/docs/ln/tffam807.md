# tffam807

LN tffam807 Transaction Summary table, Generated 2026-04-10T19:41:35Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam807` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `comp`, `acct`, `dim1`, `dim2`, `dim3`, `dim4`, `dim5`, `dims`, `book`, `lkey`, `idtc`, `year`, `prod` |
| **Column count** | 58 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `comp` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `acct` | `string` | `varchar` | `PK` | `not_null` | (required) Ledger Account. Max length: 12 |
| `dim1` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 1. Max length: 9 |
| `dim2` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 2. Max length: 9 |
| `dim3` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 3. Max length: 9 |
| `dim4` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 4. Max length: 9 |
| `dim5` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `dims` | `string` | `varchar` | `PK` | `not_null` | (required) Dimension 6-12. Max length: 63 |
| `book` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Book. Max length: 10 |
| `lkey` | `integer` | `int` | `PK` | `not_null` | (required) Location Key |
| `idtc` | `string` | `varchar` | `PK` | `not_null` | (required) Integration Document Type. Max length: 8 |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Year |
| `prod` | `integer` | `int` | `PK` | `not_null` | (required) Period |
| `acqa_1` | `float` | `float` |  |  | Acquisition |
| `acqa_2` | `float` | `float` |  |  | Acquisition |
| `acqa_3` | `float` | `float` |  |  | Acquisition |
| `adja_1` | `float` | `float` |  |  | Adjustment |
| `adja_2` | `float` | `float` |  |  | Adjustment |
| `adja_3` | `float` | `float` |  |  | Adjustment |
| `depr_1` | `float` | `float` |  |  | Depreciation |
| `depr_2` | `float` | `float` |  |  | Depreciation |
| `depr_3` | `float` | `float` |  |  | Depreciation |
| `disa_1` | `float` | `float` |  |  | Disposal |
| `disa_2` | `float` | `float` |  |  | Disposal |
| `disa_3` | `float` | `float` |  |  | Disposal |
| `proa_1` | `float` | `float` |  |  | Proceeds |
| `proa_2` | `float` | `float` |  |  | Proceeds |
| `proa_3` | `float` | `float` |  |  | Proceeds |
| `traa_1` | `float` | `float` |  |  | Transfer |
| `traa_2` | `float` | `float` |  |  | Transfer |
| `traa_3` | `float` | `float` |  |  | Transfer |
| `rcst_1` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_2` | `float` | `float` |  |  | Revaluation Cost |
| `rcst_3` | `float` | `float` |  |  | Revaluation Cost |
| `rdpr_1` | `float` | `float` |  |  | Revaluation Depreciation |
| `rdpr_2` | `float` | `float` |  |  | Revaluation Depreciation |
| `rdpr_3` | `float` | `float` |  |  | Revaluation Depreciation |
| `trct_1` | `float` | `float` |  |  | Transfer Revaluation Cost |
| `trct_2` | `float` | `float` |  |  | Transfer Revaluation Cost |
| `trct_3` | `float` | `float` |  |  | Transfer Revaluation Cost |
| `book_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam600 Books |
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

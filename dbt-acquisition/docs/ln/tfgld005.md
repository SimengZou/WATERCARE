# tfgld005

LN tfgld005 Periods table, Generated 2026-04-10T19:41:40Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld005` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ptyp`, `year`, `prno` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ptyp` | `integer` | `int` | `PK` | `not_null` | (required) Period Type. Allowed values: 1, 2, 3 |
| `ptyp_kw` | `string` | `varchar` |  |  | Period Type (keyword). Allowed values: tfgld.ptyp.financial, tfgld.ptyp.reporting, tfgld.ptyp.vat |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Year |
| `prno` | `integer` | `int` | `PK` | `not_null` | (required) Period |
| `stdt` | `date` | `date` |  |  | Period Start Date |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 15 |
| `corr` | `integer` | `int` |  |  | Correction Period. Allowed values: 1, 2 |
| `corr_kw` | `string` | `varchar` |  |  | Correction Period (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `year_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld006 End Dates by Year |
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

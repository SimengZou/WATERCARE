# tffam650

LN tffam650 Reasons table, Generated 2026-04-10T19:41:33Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam650` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `reas` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `reas` | `string` | `varchar` | `PK` | `not_null` | (required) Reason. Max length: 10 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `rety` | `integer` | `int` |  |  | Reason Type. Allowed values: 1, 2, 3, 4 |
| `rety_kw` | `string` | `varchar` |  |  | Reason Type (keyword). Allowed values: tffam.rety.adju, tffam.rety.disp, tffam.rety.remo, tffam.rety.tran |
| `cpga` | `integer` | `int` |  |  | Capital Gains. Allowed values: 1, 2 |
| `cpga_kw` | `string` | `varchar` |  |  | Capital Gains (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `impa` | `integer` | `int` |  |  | Impairment Type. Allowed values: 1, 2, 3, 4, 5 |
| `impa_kw` | `string` | `varchar` |  |  | Impairment Type (keyword). Allowed values: tffam.impa.fis.acc, tffam.impa.not.fis.acc, tffam.impa.rev.fis.acc, tffam.impa.not.rev.fis.acc, tffam.impa.not.app |
| `skev` | `integer` | `int` |  |  | SK Evaluation. Allowed values: 1, 2 |
| `skev_kw` | `string` | `varchar` |  |  | SK Evaluation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `czev` | `integer` | `int` |  |  | CZ Evaluation. Allowed values: 1, 2 |
| `czev_kw` | `string` | `varchar` |  |  | CZ Evaluation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
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

# tppdm801

LN tppdm801 Employee Project Data table, Generated 2026-04-10T19:42:03Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm801` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `emno` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `emno` | `string` | `varchar` | `PK` | `not_null` | (required) Employee. Max length: 9 |
| `ctrg` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `dcpu` | `float` | `float` |  |  | Normal Cap. Utilization by Day |
| `usyn` | `integer` | `int` |  |  | Used in Schedule. Allowed values: 1, 2 |
| `usyn_kw` | `string` | `varchar` |  |  | Used in Schedule (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ctrg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm016 Trade Groups |
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

# tcmcs048

LN tcmcs048 Cost Components table, Generated 2026-04-10T19:41:12Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs048` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cpcp` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cpcp` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Component. Max length: 8 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `cpus` | `integer` | `int` |  |  | Component Type. Allowed values: 10, 20, 30 |
| `cpus_kw` | `string` | `varchar` |  |  | Component Type (keyword). Allowed values: tccpus.detail, tccpus.collect, tccpus.aggregated |
| `iinv` | `integer` | `int` |  |  | Included in Inventory Valuation. Allowed values: 1, 2 |
| `iinv_kw` | `string` | `varchar` |  |  | Included in Inventory Valuation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fxvc` | `integer` | `int` |  |  | Fixed / Variable. Allowed values: 10, 20, 30, 100 |
| `fxvc_kw` | `string` | `varchar` |  |  | Fixed / Variable (keyword). Allowed values: tcfxvc.fixed, tcfxvc.variable, tcfxvc.both, tcfxvc.not.appl |
| `dinc` | `integer` | `int` |  |  | Direct / Indirect. Allowed values: 10, 20, 30, 100 |
| `dinc_kw` | `string` | `varchar` |  |  | Direct / Indirect (keyword). Allowed values: tcdinc.direct, tcdinc.indirect, tcdinc.not.specified, tcdinc.not.appl |
| `cref` | `integer` | `int` |  |  | Cost Type. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `cref_kw` | `string` | `varchar` |  |  | Cost Type (keyword). Allowed values: tccref.operation, tccref.material, tccref.chg.operation, tccref.chg.material, tccref.transfer, tccref.general, tccref.not.appl |
| `tpoc` | `integer` | `int` |  |  | Cost Subtype. Allowed values: 10, 20, 30, 40, 42, 45, 50, 100 |
| `tpoc_kw` | `string` | `varchar` |  |  | Cost Subtype (keyword). Allowed values: tctpoc.labor, tctpoc.machine, tctpoc.ovhd.man.hours, tctpoc.ovhd.mach.hours, tctpoc.overhead, tctpoc.subcontract, tctpoc.not.specified, tctpoc.not.appl |
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

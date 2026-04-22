# tppdm015

LN tppdm015 Labor table, Generated 2026-04-10T19:41:53Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm015` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `task` |
| **Column count** | 64 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `task` | `string` | `varchar` | `PK` | `not_null` | (required) Task. Max length: 8 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `ctrg` | `string` | `varchar` |  |  | Trade Group. Max length: 6 |
| `cuni` | `string` | `varchar` |  |  | Cost Rate Unit. Max length: 3 |
| `norm` | `float` | `float` |  |  | Norm |
| `ccts` | `string` | `varchar` |  |  | Control Code. Max length: 8 |
| `ccfu` | `integer` | `int` |  |  | Control Function. Allowed values: 1, 2, 3 |
| `ccfu_kw` | `string` | `varchar` |  |  | Control Function (keyword). Allowed values: tppdm.ccfu.c.unit, tppdm.ccfu.c.unit.cc.code, tppdm.ccfu.cc.code |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `ratc` | `float` | `float` |  |  | Cost Rate |
| `cccr` | `string` | `varchar` |  |  | Cost Rate Currency. Max length: 3 |
| `ltrc` | `timestamp` | `timestamp_ntz` |  |  | Cost Rate Transaction Date |
| `rats` | `float` | `float` |  |  | Sales Rate |
| `ccsr` | `string` | `varchar` |  |  | Sales Rate Currency. Max length: 3 |
| `ltrs` | `timestamp` | `timestamp_ntz` |  |  | Sales Rate Transaction Date |
| `rati` | `float` | `float` |  |  | Intercompany Rate |
| `ccir` | `string` | `varchar` |  |  | Intercompany Rate Currency. Max length: 3 |
| `ltri` | `timestamp` | `timestamp_ntz` |  |  | Intercompany Rate Transaction Date |
| `prre` | `integer` | `int` |  |  | Register Progress. Allowed values: 1, 2 |
| `prre_kw` | `string` | `varchar` |  |  | Register Progress (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `usyn` | `integer` | `int` |  |  | Used in Schedule. Allowed values: 1, 2 |
| `usyn_kw` | `string` | `varchar` |  |  | Used in Schedule (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `rcmc_1` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rcmc_2` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rcmc_3` | `float` | `float` |  |  | Cost Rate Multi Currency |
| `rsmc_1` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rsmc_2` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rsmc_3` | `float` | `float` |  |  | Sales Rate Multi Currency |
| `rimc_1` | `float` | `float` |  |  | Intercompany Rate Multi Currency |
| `rimc_2` | `float` | `float` |  |  | Intercompany Rate Multi Currency |
| `rimc_3` | `float` | `float` |  |  | Intercompany Rate Multi Currency |
| `unrt` | `string` | `varchar` |  |  | Rate Unit. Max length: 3 |
| `rfac` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `actp` | `integer` | `int` |  |  | Active for Planned Cost. Allowed values: 1, 2 |
| `actp_kw` | `string` | `varchar` |  |  | Active for Planned Cost (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acta` | `integer` | `int` |  |  | Active for Actual Cost. Allowed values: 1, 2 |
| `acta_kw` | `string` | `varchar` |  |  | Active for Actual Cost (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eplf` | `float` | `float` |  |  | Planning Factor |
| `cofc` | `string` | `varchar` |  |  | Department. Max length: 6 |
| `txta` | `integer` | `int` |  |  | Text |
| `ctrg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm016 Trade Groups |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cccr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccsr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccir_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `unrt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

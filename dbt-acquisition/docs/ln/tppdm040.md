# tppdm040

LN tppdm040 Sundry Costs table, Generated 2026-04-10T19:41:55Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm040` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cico` |
| **Column count** | 55 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cico` | `string` | `varchar` | `PK` | `not_null` | (required) Sundry Cost. Max length: 8 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `cccp` | `string` | `varchar` |  |  | Unit Cost Currency. Max length: 3 |
| `ltcp` | `timestamp` | `timestamp_ntz` |  |  | Unit Cost Transaction Date |
| `pris` | `float` | `float` |  |  | Sales Price |
| `ccsp` | `string` | `varchar` |  |  | Sales Price Currency. Max length: 3 |
| `ltsp` | `timestamp` | `timestamp_ntz` |  |  | Sales Price Transaction Date |
| `prii` | `float` | `float` |  |  | Intercompany Price |
| `ccip` | `string` | `varchar` |  |  | Intercompany Price Currency. Max length: 3 |
| `ltip` | `timestamp` | `timestamp_ntz` |  |  | Intercompany Price Transaction Date |
| `ccic` | `string` | `varchar` |  |  | Control Code Sundry Cost. Max length: 8 |
| `ccfu` | `integer` | `int` |  |  | Control Function. Allowed values: 1, 2, 3 |
| `ccfu_kw` | `string` | `varchar` |  |  | Control Function (keyword). Allowed values: tppdm.ccfu.c.unit, tppdm.ccfu.c.unit.cc.code, tppdm.ccfu.cc.code |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `prre` | `integer` | `int` |  |  | Register Progress. Allowed values: 1, 2 |
| `prre_kw` | `string` | `varchar` |  |  | Register Progress (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `usyn` | `integer` | `int` |  |  | Used in Schedule. Allowed values: 1, 2 |
| `usyn_kw` | `string` | `varchar` |  |  | Used in Schedule (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `pcmc_1` | `float` | `float` |  |  | Unit Cost Multi Currency |
| `pcmc_2` | `float` | `float` |  |  | Unit Cost Multi Currency |
| `pcmc_3` | `float` | `float` |  |  | Unit Cost Multi Currency |
| `psmc_1` | `float` | `float` |  |  | Sales Price Multi Currency |
| `psmc_2` | `float` | `float` |  |  | Sales Price Multi Currency |
| `psmc_3` | `float` | `float` |  |  | Sales Price Multi Currency |
| `pimc_1` | `float` | `float` |  |  | Intercompany Price Multi Currency |
| `pimc_2` | `float` | `float` |  |  | Intercompany Price Multi Currency |
| `pimc_3` | `float` | `float` |  |  | Intercompany Price Multi Currency |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `blbl` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `blbl_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `actp` | `integer` | `int` |  |  | Active for Planned Cost. Allowed values: 1, 2 |
| `actp_kw` | `string` | `varchar` |  |  | Active for Planned Cost (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acta` | `integer` | `int` |  |  | Active for Actual Cost. Allowed values: 1, 2 |
| `acta_kw` | `string` | `varchar` |  |  | Active for Actual Cost (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cccp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccsp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccip_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
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

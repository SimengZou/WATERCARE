# tpppc606

LN tpppc606 Overhead Cost Forecast table, Generated 2026-04-10T19:42:17Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc606` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `rgdt`, `ovhd`, `cspa`, `cact` |
| **Column count** | 60 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `rgdt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Registration Date |
| `ovhd` | `string` | `varchar` | `PK` | `not_null` | (required) Overhead. Max length: 8 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `rtcc_1` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtcc_2` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtcc_3` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rdat` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `appr` | `integer` | `int` |  |  | Approved. Allowed values: 1, 2 |
| `appr_kw` | `string` | `varchar` |  |  | Approved (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ovhd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm042 Standard Overhead |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `amoc_cntc` | `float` | `float` |  |  | CC: Overhead Cost Amount in Contract Currency |
| `amoc_dwhc` | `float` | `float` |  |  | CC: Overhead Cost Amount in Data Warehouse Currency |
| `amoc_refc` | `float` | `float` |  |  | CC: Overhead Cost Amount in Reference Currency |
| `amoc_homc` | `float` | `float` |  |  | CC: Overhead Cost Amount in Local Currency |
| `amoc_rpc1` | `float` | `float` |  |  | CC: Overhead Cost Amount in Reporting Currency 1 |
| `amoc_rpc2` | `float` | `float` |  |  | CC: Overhead Cost Amount in Reporting Currency 2 |
| `amoc_prjc` | `float` | `float` |  |  | CC: Overhead Cost Amount in Project Currency |
| `etoc_cntc` | `float` | `float` |  |  | CC: Estimate to Completion in Contract Currency |
| `etoc_dwhc` | `float` | `float` |  |  | CC: Estimate to Completion in Data Warehouse Currency |
| `etoc_refc` | `float` | `float` |  |  | CC: Estimate to Completion in Reference Currency |
| `etoc_homc` | `float` | `float` |  |  | CC: Estimate to Completion in Local Currency |
| `etoc_rpc1` | `float` | `float` |  |  | CC: Estimate to Completion in Reporting Currency 1 |
| `etoc_rpc2` | `float` | `float` |  |  | CC: Estimate to Completion in Reporting Currency 2 |
| `etoc_prjc` | `float` | `float` |  |  | CC: Estimate to Completion in Project Currency |
| `etoc_trnc` | `float` | `float` |  |  | CC: Estimate to Completion in Transaction Currency |
| `eatc_cntc` | `float` | `float` |  |  | CC: Estimate at Completion in Contract Currency |
| `eatc_dwhc` | `float` | `float` |  |  | CC: Estimate at Completion in Data Warehouse Currency |
| `eatc_refc` | `float` | `float` |  |  | CC: Estimate at Completion in Reference Currency |
| `eatc_homc` | `float` | `float` |  |  | CC: Estimate at Completion in Local Currency |
| `eatc_rpc1` | `float` | `float` |  |  | CC: Estimate at Completion in Reporting Currency 1 |
| `eatc_rpc2` | `float` | `float` |  |  | CC: Estimate at Completion in Reporting Currency 2 |
| `eatc_prjc` | `float` | `float` |  |  | CC: Estimate at Completion in Project Currency |
| `eatc_trnc` | `float` | `float` |  |  | CC: Estimate at Completion in Transaction Currency |
| `cprj_fcmp` | `integer` | `int` |  |  | CC: Project Financial Company |
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

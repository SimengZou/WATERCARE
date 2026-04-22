# tpptc160

LN tpptc160 Sundry Costs Budget Lines table, Generated 2026-04-10T19:42:23Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc160` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cspa`, `sern` |
| **Column count** | 59 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cspa` | `string` | `varchar` | `PK` | `not_null` | (required) Element. Max length: 16 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `cico` | `string` | `varchar` |  |  | Sundry Cost. Max length: 8 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `quan` | `float` | `float` |  |  | Quantity |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `cocu` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `pric` | `float` | `float` |  |  | Unit Cost |
| `btdt` | `timestamp` | `timestamp_ntz` |  |  | Budget Date |
| `pris` | `float` | `float` |  |  | Sales Price |
| `sacu` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `dfpc` | `integer` | `int` |  |  | Final Unit Cost. Allowed values: 1, 2 |
| `dfpc_kw` | `string` | `varchar` |  |  | Final Unit Cost (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `amos` | `float` | `float` |  |  | Sales Amount |
| `stat` | `integer` | `int` |  |  | Budget Status. Allowed values: 1, 2, 3 |
| `stat_kw` | `string` | `varchar` |  |  | Budget Status (keyword). Allowed values: tpptc.stat.free, tpptc.stat.actual, tpptc.stat.definite |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `rtcc_1` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtcc_2` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtcc_3` | `float` | `float` |  |  | Currency Rate (Costs) |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor (Costs) |
| `rtcs_1` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtcs_2` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtcs_3` | `float` | `float` |  |  | Currency Rate (Sls) |
| `rtfs_1` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_2` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `rtfs_3` | `integer` | `int` |  |  | Rate Factor (Sales) |
| `tsrl` | `integer` | `int` |  |  | Service Related. Allowed values: 1, 2 |
| `tsrl_kw` | `string` | `varchar` |  |  | Service Related (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `trts` | `integer` | `int` |  |  | Transferred to Service. Allowed values: 1, 2 |
| `trts_kw` | `string` | `varchar` |  |  | Transferred to Service (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cona` | `float` | `float` |  |  | Contingency Amount |
| `txta` | `integer` | `int` |  |  | Text |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cocu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `sacu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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

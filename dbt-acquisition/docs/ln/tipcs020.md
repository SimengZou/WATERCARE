# tipcs020

LN tipcs020 General Project Data table, Generated 2026-04-10T19:41:47Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tipcs020` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj` |
| **Column count** | 32 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference - base language. Max length: 20 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `kopr` | `integer` | `int` |  |  | Project Type. Allowed values: 1, 2, 3, 4 |
| `kopr_kw` | `string` | `varchar` |  |  | Project Type (keyword). Allowed values: tckopr.main.project, tckopr.sub.project, tckopr.single.project, tckopr.budget |
| `ccgr` | `string` | `varchar` |  |  | Calculation Group. Max length: 6 |
| `bpid` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `pemp` | `string` | `varchar` |  |  | Project Employee. Max length: 9 |
| `ffci` | `integer` | `int` |  |  | First Free Item Code |
| `cpcc` | `string` | `varchar` |  |  | Cost Calculation Code. Max length: 3 |
| `clco` | `string` | `varchar` |  |  | Calculation Office. Max length: 6 |
| `peng` | `integer` | `int` |  |  | Engineering Allowed. Allowed values: 0, 1, 2 |
| `peng_kw` | `string` | `varchar` |  |  | Engineering Allowed (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `cfdt` | `timestamp` | `timestamp_ntz` |  |  | Reference Date |
| `dtfs` | `integer` | `int` |  |  | Date Filled by Sales. Allowed values: 1, 2 |
| `dtfs_kw` | `string` | `varchar` |  |  | Date Filled by Sales (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `ccgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tipcs010 Calculation Groups |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `pemp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cpcc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ticpr100 Cost Calculation Codes |
| `clco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
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

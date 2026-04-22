# tpptc316

LN tpptc316 Actual Budget by Activity/Cost Component table, Generated 2026-04-10T19:42:26Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc316` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `ccal`, `cpla`, `cact`, `ccco` |
| **Column count** | 45 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ccal` | `string` | `varchar` | `PK` | `not_null` | (required) Budget Cost Analysis Version. Max length: 3 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `ccco` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Component. Max length: 8 |
| `amac_1` | `float` | `float` |  |  | Item Cost |
| `amac_2` | `float` | `float` |  |  | Item Cost |
| `amac_3` | `float` | `float` |  |  | Item Cost |
| `amac_4` | `float` | `float` |  |  | Item Cost |
| `atac_1` | `float` | `float` |  |  | Labor Amount |
| `atac_2` | `float` | `float` |  |  | Labor Amount |
| `atac_3` | `float` | `float` |  |  | Labor Amount |
| `atac_4` | `float` | `float` |  |  | Labor Amount |
| `qtah` | `float` | `float` |  |  | No. of Labor Hours |
| `aeqc_1` | `float` | `float` |  |  | Equipment Amount |
| `aeqc_2` | `float` | `float` |  |  | Equipment Amount |
| `aeqc_3` | `float` | `float` |  |  | Equipment Amount |
| `aeqc_4` | `float` | `float` |  |  | Equipment Amount |
| `asbc_1` | `float` | `float` |  |  | Subcontracting Amount |
| `asbc_2` | `float` | `float` |  |  | Subcontracting Amount |
| `asbc_3` | `float` | `float` |  |  | Subcontracting Amount |
| `asbc_4` | `float` | `float` |  |  | Subcontracting Amount |
| `aicl_1` | `float` | `float` |  |  | Sundry Cost Amount |
| `aicl_2` | `float` | `float` |  |  | Sundry Cost Amount |
| `aicl_3` | `float` | `float` |  |  | Sundry Cost Amount |
| `aicl_4` | `float` | `float` |  |  | Sundry Cost Amount |
| `aicc_1` | `float` | `float` |  |  | Sundry Cost Amount |
| `aicc_2` | `float` | `float` |  |  | Sundry Cost Amount |
| `aicc_3` | `float` | `float` |  |  | Sundry Cost Amount |
| `aicc_4` | `float` | `float` |  |  | Sundry Cost Amount |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc300 Budget Cost Analysis Versions by Project |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
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

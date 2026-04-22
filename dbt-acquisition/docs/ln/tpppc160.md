# tpppc160

LN tpppc160 Activity Physical Progress table, Generated 2026-04-10T19:42:06Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc160` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cpla`, `cact`, `date` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `date` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Date |
| `prst` | `float` | `float` |  |  | Physical Progress |
| `quan` | `float` | `float` |  |  | Progress (Quantity) |
| `prco` | `integer` | `int` |  |  | Reported Completed. Allowed values: 0, 1, 2 |
| `prco_kw` | `string` | `varchar` |  |  | Reported Completed (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `appr` | `integer` | `int` |  |  | Approved. Allowed values: 1, 2 |
| `appr_kw` | `string` | `varchar` |  |  | Approved (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `txtn` | `integer` | `int` |  |  | Text |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `txtn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

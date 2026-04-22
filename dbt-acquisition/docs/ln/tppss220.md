# tppss220

LN tppss220 Activity Baselines table, Generated 2026-04-10T19:42:19Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppss220` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `basn`, `cact` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `basn` | `integer` | `int` | `PK` | `not_null` | (required) Baseline |
| `cact` | `string` | `varchar` | `PK` | `not_null` | (required) Activity. Max length: 16 |
| `pact` | `string` | `varchar` |  |  | Parent Activity. Max length: 16 |
| `tact` | `integer` | `int` |  |  | Activity Type. Allowed values: 0, 1, 2, 3, 4, 5 |
| `tact_kw` | `string` | `varchar` |  |  | Activity Type (keyword). Allowed values: , tppdm.tact.wbsel, tppdm.tact.cosac, tppdm.tact.plapa, tppdm.tact.worpa, tppdm.tact.milst |
| `bsdt` | `timestamp` | `timestamp_ntz` |  |  | Baseline Start Date |
| `bfdt` | `timestamp` | `timestamp_ntz` |  |  | Baseline Finish Date |
| `cprj_basn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss020 Baselines |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `actv_cpla` | `string` | `varchar` |  |  | CC: Project Active Plan. Max length: 3 |
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

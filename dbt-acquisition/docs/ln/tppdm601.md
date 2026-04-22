# tppdm601

LN tppdm601 Cost Control Levels by Project table, Generated 2026-04-10T19:41:59Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppdm601` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `ccbl` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ccbl` | `integer` | `int` | `PK` | `not_null` | (required) Cost Control Level. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 |
| `ccbl_kw` | `string` | `varchar` |  |  | Cost Control Level (keyword). Allowed values: tppdm.ccbl.project, tppdm.ccbl.proj.costtype, tppdm.ccbl.proj.costcomp, tppdm.ccbl.proj.costunit, tppdm.ccbl.strucp, tppdm.ccbl.strucp.costtype, tppdm.ccbl.strucp.costcomp, tppdm.ccbl.strucp.costunit, tppdm.ccbl.activ, tppdm.ccbl.activ.costtype, tppdm.ccbl.activ.costcomp, tppdm.ccbl.activ.costunit, tppdm.ccbl.settlm, tppdm.ccbl.settlm.costtype, tppdm.ccbl.settlm.costcomp, tppdm.ccbl.settlm.costunit |
| `coco` | `integer` | `int` |  |  | Cost Control. Allowed values: 1, 2 |
| `coco_kw` | `string` | `varchar` |  |  | Cost Control (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
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

# tpptc300

LN tpptc300 Budget Cost Analysis Versions by Project table, Generated 2026-04-10T19:42:25Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc300` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `ccal` |
| **Column count** | 38 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `ccal` | `string` | `varchar` | `PK` | `not_null` | (required) Budget Cost Analysis Version. Max length: 3 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `ibud` | `integer` | `int` |  |  | Initial Budget. Allowed values: 0, 1, 2 |
| `ibud_kw` | `string` | `varchar` |  |  | Initial Budget (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `upmd` | `integer` | `int` |  |  | Update Mode. Allowed values: 0, 1, 2 |
| `upmd_kw` | `string` | `varchar` |  |  | Update Mode (keyword). Allowed values: , tppss.plmd.update, tppss.plmd.read.only |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lcdt` | `timestamp` | `timestamp_ntz` |  |  | Date of Last Update |
| `lclg` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `bdtp` | `integer` | `int` |  |  | Budget Type. Allowed values: 1, 2 |
| `bdtp_kw` | `string` | `varchar` |  |  | Budget Type (keyword). Allowed values: tppdm.bdtp.struct.budget, tppdm.bdtp.activity.budget |
| `free` | `integer` | `int` |  |  | Budget Status 'Free'. Allowed values: 0, 1, 2 |
| `free_kw` | `string` | `varchar` |  |  | Budget Status 'Free' (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `actl` | `integer` | `int` |  |  | Budget Status 'Actual'. Allowed values: 0, 1, 2 |
| `actl_kw` | `string` | `varchar` |  |  | Budget Status 'Actual' (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `defn` | `integer` | `int` |  |  | Budget Status 'Final'. Allowed values: 0, 1, 2 |
| `defn_kw` | `string` | `varchar` |  |  | Budget Status 'Final' (keyword). Allowed values: , tppdm.yeno.yes, tppdm.yeno.no |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cexc` | `string` | `varchar` |  |  | Exchange Rate Type Budget. Max length: 3 |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `iexc` | `integer` | `int` |  |  | Including Extension Costs. Allowed values: 1, 2 |
| `iexc_kw` | `string` | `varchar` |  |  | Including Extension Costs (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `icon` | `integer` | `int` |  |  | Including Contingency. Allowed values: 1, 2 |
| `icon_kw` | `string` | `varchar` |  |  | Including Contingency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cexc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
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

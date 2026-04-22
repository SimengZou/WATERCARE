# tppss210

LN tppss210 Activity Relationships table, Generated 2026-04-10T19:42:19Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tppss210` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `cpla`, `ffno` |
| **Column count** | 33 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `cpla` | `string` | `varchar` | `PK` | `not_null` | (required) Plan. Max length: 3 |
| `ffno` | `string` | `varchar` | `PK` | `not_null` | (required) First Free Number. Max length: 9 |
| `pact` | `string` | `varchar` |  |  | Predecessor Activity. Max length: 16 |
| `pcot` | `integer` | `int` |  |  | Predecessor Cost Type. Allowed values: 1, 2, 3, 4, 5, 6 |
| `pcot_kw` | `string` | `varchar` |  |  | Predecessor Cost Type (keyword). Allowed values: tppdm.cote.tasks, tppdm.cote.materials, tppdm.cote.equipment, tppdm.cote.subcontracting, tppdm.cote.indirect, tppdm.cote.empty |
| `psen` | `integer` | `int` |  |  | Predecessor Budget Line Number |
| `sact` | `string` | `varchar` |  |  | Successor Activity. Max length: 16 |
| `scot` | `integer` | `int` |  |  | Successor Cost Type. Allowed values: 1, 2, 3, 4, 5, 6 |
| `scot_kw` | `string` | `varchar` |  |  | Successor Cost Type (keyword). Allowed values: tppdm.cote.tasks, tppdm.cote.materials, tppdm.cote.equipment, tppdm.cote.subcontracting, tppdm.cote.indirect, tppdm.cote.empty |
| `ssen` | `integer` | `int` |  |  | Successor Budget Line Number |
| `rltp` | `integer` | `int` |  |  | Relationship Type. Allowed values: 1, 2, 3, 4 |
| `rltp_kw` | `string` | `varchar` |  |  | Relationship Type (keyword). Allowed values: tppss.rsty.stst, tppss.rsty.stfi, tppss.rsty.fist, tppss.rsty.fifi |
| `lead` | `float` | `float` |  |  | Lead/Lag |
| `cuni` | `string` | `varchar` |  |  | Lead/Lag Unit. Max length: 3 |
| `llpc` | `integer` | `int` |  |  | Lead/Lag Percentage |
| `rest` | `integer` | `int` |  |  | Linked to ESP. Allowed values: 1, 2 |
| `rest_kw` | `string` | `varchar` |  |  | Linked to ESP (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cprj_cpla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss010 Project Plans |
| `cprj_cpla_pact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_cpla_sact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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

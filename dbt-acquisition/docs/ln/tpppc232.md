# tpppc232

LN tpppc232 Subcontracting Hours table, Generated 2026-04-10T19:42:08Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpppc232` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cpth`, `year`, `peri`, `otbp`, `sern` |
| **Column count** | 49 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cpth` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Period Table Hours. Max length: 6 |
| `year` | `integer` | `int` | `PK` | `not_null` | (required) Hours Control Year |
| `peri` | `integer` | `int` | `PK` | `not_null` | (required) Hours Control Period |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `csub` | `string` | `varchar` |  |  | Subcontracting. Max length: 47 |
| `task` | `string` | `varchar` |  |  | Labor. Max length: 8 |
| `ltdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Time |
| `rgdt` | `timestamp` | `timestamp_ntz` |  |  | Registration Date |
| `quan` | `float` | `float` |  |  | No. of Hours |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `cdoc__en_us` | `string` | `varchar` |  | `not_null` | (required) Document - base language. Max length: 10 |
| `cptc` | `string` | `varchar` |  |  | Cost Period Table for Cost Control Period. Max length: 6 |
| `cyea` | `integer` | `int` |  |  | Cost Control Year |
| `cper` | `integer` | `int` |  |  | Cost Control Period |
| `cfpo` | `integer` | `int` |  |  | Approved for Posting. Allowed values: 1, 2 |
| `cfpo_kw` | `string` | `varchar` |  |  | Approved for Posting (keyword). Allowed values: tppdm.yeno.yes, tppdm.yeno.no |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `orno` | `string` | `varchar` |  |  | Purchase Order. Max length: 9 |
| `otbp` | `string` | `varchar` | `PK` | `not_null` | (required) Subcontractor. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Position Number |
| `loco` | `string` | `varchar` |  |  | User. Max length: 16 |
| `txta` | `integer` | `int` |  |  | Text |
| `cpth_year_peri_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cpth_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `cptc_cyea_cper_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp070 Periods |
| `cptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp060 Period Tables |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
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

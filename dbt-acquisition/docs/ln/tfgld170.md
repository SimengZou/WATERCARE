# tfgld170

LN tfgld170 Taxonomies table, Generated 2026-04-10T19:41:42Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld170` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `taxo`, `vers` |
| **Column count** | 33 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `taxo` | `string` | `varchar` | `PK` | `not_null` | (required) Taxonomy. Max length: 9 |
| `vers` | `integer` | `int` | `PK` | `not_null` | (required) Version |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 10, 20, 30, 40, 50 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tfgld.taxo.sta.draft, tfgld.taxo.sta.validation.err, tfgld.taxo.sta.validated, tfgld.taxo.sta.active, tfgld.taxo.sta.closed |
| `duac` | `integer` | `int` |  |  | Dual Accounting Indicator. Allowed values: 0, 1, 2, 3 |
| `duac_kw` | `string` | `varchar` |  |  | Dual Accounting Indicator (keyword). Allowed values: , tfgld.duac.pr.statutory, tfgld.duac.pr.complem, tfgld.duac.pr.both |
| `acom` | `integer` | `int` |  |  | All Companies. Allowed values: 0, 1, 2 |
| `acom_kw` | `string` | `varchar` |  |  | All Companies (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `dimm` | `integer` | `int` |  |  | Dimensions Used in Mapping. Allowed values: 0, 1, 2 |
| `dimm_kw` | `string` | `varchar` |  |  | Dimensions Used in Mapping (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `usrc` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `usrm` | `string` | `varchar` |  |  | Modified by. Max length: 16 |
| `datm` | `timestamp` | `timestamp_ntz` |  |  | Modification Date |
| `usrr` | `string` | `varchar` |  |  | Released by. Max length: 16 |
| `rldt` | `timestamp` | `timestamp_ntz` |  |  | Release Date |
| `usre` | `string` | `varchar` |  |  | Closed by. Max length: 16 |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Closing Date |
| `eacc` | `integer` | `int` |  |  | e-Accounting. Allowed values: 1, 2 |
| `eacc_kw` | `string` | `varchar` |  |  | e-Accounting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `text` | `integer` | `int` |  |  | Text |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

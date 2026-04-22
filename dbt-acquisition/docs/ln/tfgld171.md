# tfgld171

LN tfgld171 Taxonomy Accounts table, Generated 2026-04-10T19:41:43Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld171` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `taxo`, `vers`, `acid` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `taxo` | `string` | `varchar` | `PK` | `not_null` | (required) Taxonomy. Max length: 9 |
| `vers` | `integer` | `int` | `PK` | `not_null` | (required) Version |
| `acid` | `string` | `varchar` | `PK` | `not_null` | (required) Account. Max length: 12 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `subl` | `integer` | `int` |  |  | Sublevel |
| `paci` | `string` | `varchar` |  |  | Parent Account. Max length: 12 |
| `acty` | `integer` | `int` |  |  | Account Type. Allowed values: 10, 20, 30, 40, 50, 60, 70, 80 |
| `acty_kw` | `string` | `varchar` |  |  | Account Type (keyword). Allowed values: tfgld.taxo.typ.asset, tfgld.taxo.typ.liability, tfgld.taxo.typ.income, tfgld.taxo.typ.expense, tfgld.taxo.typ.intercompany, tfgld.taxo.typ.intersegment, tfgld.taxo.typ.text, tfgld.taxo.typ.miscellaneous |
| `mapp` | `integer` | `int` |  |  | Mapping Details. Allowed values: 1, 2 |
| `mapp_kw` | `string` | `varchar` |  |  | Mapping Details (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `unas` | `integer` | `int` |  |  | Unassigned Account. Allowed values: 1, 2 |
| `unas_kw` | `string` | `varchar` |  |  | Unassigned Account (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pseq` | `integer` | `int` |  |  | Print Sequence |
| `text` | `integer` | `int` |  |  | Text |
| `taxo_vers_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld170 Taxonomies |
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

# tfgld172

LN tfgld172 Mappings by Taxonomy Account table, Generated 2026-04-10T19:41:43Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld172` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `taxo`, `vers`, `acid`, `line` |
| **Column count** | 43 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `taxo` | `string` | `varchar` | `PK` | `not_null` | (required) Taxonomy. Max length: 9 |
| `vers` | `integer` | `int` | `PK` | `not_null` | (required) Version |
| `acid` | `string` | `varchar` | `PK` | `not_null` | (required) Account. Max length: 12 |
| `line` | `integer` | `int` | `PK` | `not_null` | (required) Line Number |
| `fcom` | `integer` | `int` |  |  | From Company |
| `tcom` | `integer` | `int` |  |  | To Company |
| `flac` | `string` | `varchar` |  |  | From Ledger Account. Max length: 12 |
| `tlac` | `string` | `varchar` |  |  | To Ledger Account. Max length: 12 |
| `fdim_1` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_2` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_3` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_4` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_5` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_6` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_7` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_8` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_9` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_10` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_11` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `fdim_12` | `string` | `varchar` |  |  | From Dimensions. Max length: 9 |
| `tdim_1` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_2` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_3` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_4` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_5` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_6` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_7` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_8` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_9` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_10` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_11` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `tdim_12` | `string` | `varchar` |  |  | To Dimensions. Max length: 9 |
| `taxo_vers_acid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld171 Taxonomy Accounts |
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

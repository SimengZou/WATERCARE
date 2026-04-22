# whina123

LN whina123 Inventory Revaluation Transaction Cost Details table, Generated 2026-04-10T19:42:46Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whina123` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `rorg`, `orno`, `seqn`, `cpcp` |
| **Column count** | 21 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `rorg` | `integer` | `int` | `PK` | `not_null` | (required) Revaluation Origin. Allowed values: 10, 20, 30, 35, 40, 50, 100 |
| `rorg_kw` | `string` | `varchar` |  |  | Revaluation Origin (keyword). Allowed values: whina.rorg.act.cost.price, whina.rorg.antedating, whina.rorg.mauc.correct, whina.rorg.act.cost.corr, whina.rorg.change.val.meth, whina.rorg.change.int.fam, whina.rorg.not.appl |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Revaluation Order. Max length: 9 |
| `seqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `cpcp` | `string` | `varchar` | `PK` | `not_null` | (required) Cost Component. Max length: 8 |
| `hour` | `float` | `float` |  |  | Hours |
| `amnt_1` | `float` | `float` |  |  | Amount |
| `amnt_2` | `float` | `float` |  |  | Amount |
| `amnt_3` | `float` | `float` |  |  | Amount |
| `rorg_orno_seqn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina122 Inventory Revaluation Transactions |
| `cpcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
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

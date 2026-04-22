# tpptc137

LN tpptc137 Control Data Labor Lines table, Generated 2026-04-10T19:42:22Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tpptc137` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cprj`, `task`, `sern` |
| **Column count** | 37 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cprj` | `string` | `varchar` | `PK` | `not_null` | (required) Project. Max length: 9 |
| `task` | `string` | `varchar` | `PK` | `not_null` | (required) Labor. Max length: 8 |
| `sern` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cpla` | `string` | `varchar` |  |  | Plan. Max length: 3 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `ccts` | `string` | `varchar` |  |  | Control Code Labor. Max length: 8 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `bsqn` | `integer` | `int` |  |  | Budget Line Number |
| `qtah` | `float` | `float` |  |  | No. of Labor Hours |
| `quan` | `float` | `float` |  |  | Quantity |
| `amoc` | `float` | `float` |  |  | Cost Amount |
| `cocu` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rtcc_1` | `float` | `float` |  |  | Cost Rate |
| `rtcc_2` | `float` | `float` |  |  | Cost Rate |
| `rtcc_3` | `float` | `float` |  |  | Cost Rate |
| `rtfc_1` | `integer` | `int` |  |  | Rate Factor |
| `rtfc_2` | `integer` | `int` |  |  | Rate Factor |
| `rtfc_3` | `integer` | `int` |  |  | Rate Factor |
| `btdt` | `timestamp` | `timestamp_ntz` |  |  | Budget Date |
| `cprj_cstl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc050 Extensions |
| `cprj_cspa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tpptc100 Elements |
| `cprj_cpla_cact_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppss200 Activities |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tppdm600 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cocu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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

# cube_ledger

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_cube_ledger` |
| **Materialization** | `incremental` |
| **Primary keys** | `version`, `year`, `period`, `ldgtrxtype`, `currency`, `analysis01`, `analysis02`, `analysis03`, `analysis04`, `analysis05`, `analysis06`, `analysis07`, `analysis08`, `analysis09`, `analysis10`, `account`, `ledgermeas` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `version` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `year` | `string` | `varchar` | `PK` |  | Max length: 150 |
| `period` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `ldgtrxtype` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `currency` | `string` | `varchar` | `PK` |  | Max length: 50 |
| `analysis01` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis02` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis03` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis04` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis05` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis06` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis07` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis08` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis09` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `analysis10` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `_account` | `string` | `varchar` |  |  | Max length: 100 |
| `ledgermeas` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `value` | `string` | `varchar` |  |  | Max length: 2000 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  |  |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

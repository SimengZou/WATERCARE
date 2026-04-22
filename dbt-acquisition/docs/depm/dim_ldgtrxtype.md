# dim_ldgtrxtype

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_dim_ldgtrxtype` |
| **Materialization** | `incremental` |
| **Primary keys** | `elementname`, `parent` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `elementname` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `parent` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `name` | `string` | `varchar` |  |  | Max length: 260 |
| `shortname` | `string` | `varchar` |  |  | Max length: 260 |
| `active` | `string` | `varchar` |  |  | Max length: 20 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  |  |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

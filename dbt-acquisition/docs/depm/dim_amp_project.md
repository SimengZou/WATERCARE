# dim_amp_project

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_dim_amp_project` |
| **Materialization** | `incremental` |
| **Primary keys** | `elementname`, `parent` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `elementname` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `parent` | `string` | `varchar` | `PK` |  | Max length: 200 |
| `name` | `string` | `varchar` |  |  | Max length: 260 |
| `shortname` | `string` | `varchar` |  |  | Max length: 260 |
| `active` | `string` | `varchar` |  |  | Max length: 20 |
| `businessarea` | `string` | `varchar` |  |  | Max length: 250 |
| `ampcode` | `string` | `varchar` |  |  | Max length: 250 |
| `emcode` | `string` | `varchar` |  |  | Max length: 250 |
| `programmanager` | `string` | `varchar` |  |  | Max length: 250 |
| `projectmanager` | `string` | `varchar` |  |  | Max length: 250 |
| `growth` | `string` | `varchar` |  |  | Max length: 250 |
| `replacement` | `string` | `varchar` |  |  | Max length: 250 |
| `levelofservice` | `string` | `varchar` |  |  | Max length: 250 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  |  |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

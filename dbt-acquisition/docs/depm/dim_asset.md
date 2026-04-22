# dim_asset

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_dim_asset` |
| **Materialization** | `incremental` |
| **Primary keys** | `elementname` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `elementname` | `string` | `varchar` | `PK` | `unique` | Max length: 200 |
| `name` | `string` | `varchar` |  |  | Max length: 300 |
| `longname` | `string` | `varchar` |  |  | Max length: 300 |
| `project` | `string` | `varchar` |  |  | Max length: 200 |
| `area` | `string` | `varchar` |  |  | Max length: 300 |
| `subarea` | `string` | `varchar` |  |  | Max length: 300 |
| `unittype` | `string` | `varchar` |  |  | Max length: 300 |
| `typedesc` | `string` | `varchar` |  |  | Max length: 300 |
| `typecode` | `string` | `varchar` |  |  | Max length: 300 |
| `life` | `string` | `varchar` |  |  | Max length: 50 |
| `servicestatus` | `string` | `varchar` |  |  | Max length: 50 |
| `model` | `string` | `varchar` |  |  | Max length: 100 |
| `serial` | `string` | `varchar` |  |  | Max length: 50 |
| `installdate` | `string` | `varchar` |  |  | Max length: 50 |
| `asbuilt` | `string` | `varchar` |  |  | Max length: 50 |
| `costcentre` | `string` | `varchar` |  |  | Max length: 50 |
| `acqtype` | `string` | `varchar` |  |  | Max length: 50 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  |  |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

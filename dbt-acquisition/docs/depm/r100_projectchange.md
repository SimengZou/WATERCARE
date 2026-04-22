# r100_projectchange

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_r100_projectchange` |
| **Materialization** | `incremental` |
| **Primary keys** | `documentid`, `project` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `documentdatetime` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) |
| `documentid` | `string` | `varchar` | `PK` | `not_null` | (required) |
| `project` | `string` | `varchar` | `PK` | `not_null` | (required) |
| `status` | `string` | `varchar` |  | `not_null` | (required) |
| `description` | `string` | `varchar` |  |  |  |
| `riskcategory` | `string` | `varchar` |  |  |  |
| `note` | `string` | `varchar` |  |  |  |
| `risknum` | `string` | `varchar` |  |  |  |
| `targetclosuredate` | `timestamp` | `timestamp_ntz` |  |  |  |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

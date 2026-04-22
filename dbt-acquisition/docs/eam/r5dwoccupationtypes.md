# r5dwoccupationtypes

eam_R5DWOCCUPATIONTYPES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwoccupationtypes` |
| **Materialization** | `incremental` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zot_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZOT_LASTSAVED |
| `zot_rdesc` | `string` | `varchar` |  |  | ZOT_RDESC |
| `zot_desc` | `string` | `varchar` |  |  | ZOT_DESC |
| `zot_rcode` | `string` | `varchar` |  |  | ZOT_RCODE |
| `zot_key` | `float` | `float` |  |  | ZOT_KEY |
| `zot_code` | `string` | `varchar` |  |  | ZOT_CODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

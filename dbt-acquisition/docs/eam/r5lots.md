# r5lots

eam_R5LOTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5lots` |
| **Materialization** | `incremental` |
| **Primary keys** | `lot_code` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lot_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LOT_LASTSAVED |
| `lot_desc` | `string` | `varchar` |  |  | LOT_DESC |
| `lot_class` | `string` | `varchar` |  |  | LOT_CLASS |
| `lot_expired` | `timestamp` | `timestamp_ntz` |  |  | LOT_EXPIRED |
| `lot_manlot` | `string` | `varchar` |  |  | LOT_MANLOT |
| `lot_class_org` | `string` | `varchar` |  |  | LOT_CLASS_ORG |
| `lot_updatecount` | `float` | `float` |  |  | LOT_UPDATECOUNT |
| `lot_buildkittrans` | `string` | `varchar` |  |  | LOT_BUILDKITTRANS |
| `lot_breakupkittrans` | `string` | `varchar` |  |  | LOT_BREAKUPKITTRANS |
| `lot_code` | `string` | `varchar` | `PK` | `unique` | LOT_CODE |
| `lot_org` | `string` | `varchar` |  |  | LOT_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

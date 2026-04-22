# r5findings

eam_R5FINDINGS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5findings` |
| **Materialization** | `incremental` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `fnd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | FND_LASTSAVED |
| `fnd_desc` | `string` | `varchar` |  |  | FND_DESC |
| `fnd_class` | `string` | `varchar` |  |  | FND_CLASS |
| `fnd_updated` | `timestamp` | `timestamp_ntz` |  |  | FND_UPDATED |
| `fnd_class_org` | `string` | `varchar` |  |  | FND_CLASS_ORG |
| `fnd_updatecount` | `float` | `float` |  |  | FND_UPDATECOUNT |
| `fnd_created` | `timestamp` | `timestamp_ntz` |  |  | FND_CREATED |
| `fnd_code` | `string` | `varchar` |  |  | FND_CODE |
| `fnd_gen` | `string` | `varchar` |  |  | FND_GEN |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

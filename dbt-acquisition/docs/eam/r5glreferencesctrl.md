# r5glreferencesctrl

eam_R5GLREFERENCESCTRL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5glreferencesctrl` |
| **Materialization** | `incremental` |
| **Primary keys** | `grc_r5column` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `grc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | GRC_LASTSAVED |
| `grc_displayname` | `string` | `varchar` |  |  | GRC_DISPLAYNAME |
| `grc_length` | `float` | `float` |  |  | GRC_LENGTH |
| `grc_displayseq` | `float` | `float` |  |  | GRC_DISPLAYSEQ |
| `grc_updatecount` | `float` | `float` |  |  | GRC_UPDATECOUNT |
| `grc_r5column` | `string` | `varchar` | `PK` | `unique` | GRC_R5COLUMN |
| `grc_datatype` | `string` | `varchar` |  |  | GRC_DATATYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

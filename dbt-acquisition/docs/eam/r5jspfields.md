# r5jspfields

eam_R5JSPFIELDS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5jspfields` |
| **Materialization** | `incremental` |
| **Primary keys** | `jfd_jspfile` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `jfd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | JFD_LASTSAVED |
| `jfd_fields` | `string` | `varchar` |  |  | JFD_FIELDS. Max length: 0 |
| `jfd_jsincludes` | `string` | `varchar` |  |  | JFD_JSINCLUDES |
| `jfd_updatecount` | `float` | `float` |  |  | JFD_UPDATECOUNT |
| `jfd_jspfile` | `string` | `varchar` | `PK` | `unique` | JFD_JSPFILE |
| `jfd_otherfields` | `string` | `varchar` |  |  | JFD_OTHERFIELDS. Max length: 0 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

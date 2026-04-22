# r5orgtabcolumns

eam_R5ORGTABCOLUMNS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5orgtabcolumns` |
| **Materialization** | `incremental` |
| **Primary keys** | `otc_table`, `otc_column` |
| **Column count** | 10 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `otc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OTC_LASTSAVED |
| `otc_updatecount` | `float` | `float` |  |  | OTC_UPDATECOUNT |
| `otc_table` | `string` | `varchar` | `PK` |  | OTC_TABLE |
| `otc_column` | `string` | `varchar` | `PK` |  | OTC_COLUMN |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

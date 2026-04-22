# r5pagecache

eam_R5PAGECACHE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pagecache` |
| **Materialization** | `incremental` |
| **Primary keys** | `pgc_usergroup`, `pgc_function`, `pgc_tabname`, `pgc_layouttype` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pgc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PGC_LASTSAVED |
| `pgc_function` | `string` | `varchar` | `PK` |  | PGC_FUNCTION |
| `pgc_tabname` | `string` | `varchar` | `PK` |  | PGC_TABNAME |
| `pgc_layouttype` | `string` | `varchar` | `PK` |  | PGC_LAYOUTTYPE |
| `pgc_jspfile` | `string` | `varchar` |  |  | PGC_JSPFILE |
| `pgc_updatecount` | `float` | `float` |  |  | PGC_UPDATECOUNT |
| `pgc_usergroup` | `string` | `varchar` | `PK` |  | PGC_USERGROUP |
| `pgc_rentity` | `string` | `varchar` |  |  | PGC_RENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5userdefinedfielddependencies

eam_R5USERDEFINEDFIELDDEPENDENCIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userdefinedfielddependencies` |
| **Materialization** | `incremental` |
| **Primary keys** | `ufd_rentity`, `ufd_pagename` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ufd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UFD_LASTSAVED |
| `ufd_pagename` | `string` | `varchar` | `PK` |  | UFD_PAGENAME |
| `ufd_layoutcache` | `string` | `varchar` |  |  | UFD_LAYOUTCACHE |
| `ufd_updatecount` | `float` | `float` |  |  | UFD_UPDATECOUNT |
| `ufd_rentity` | `string` | `varchar` | `PK` |  | UFD_RENTITY |
| `ufd_gridcache` | `string` | `varchar` |  |  | UFD_GRIDCACHE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |
